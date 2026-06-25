"""Tests for kube backend graceful degradation.

Covers:
- Issue #504: metrics server absence should not break the stat reporter/display
- Issue #301: PVC deletion should handle 404 errors gracefully
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from stimela.monitoring.kube import KubeReport

# ---------------------------------------------------------------------------
# Issue #504 – KubeReport.profiling_results with None metrics
# ---------------------------------------------------------------------------


class TestKubeReportProfilingResults:
    """KubeReport.profiling_results must tolerate None metric values."""

    def test_profiling_results_with_values(self):
        report = KubeReport(total_cores=2.5, total_memory=16.0)
        result = report.profiling_results
        assert result == {"k8s_cores": 2.5, "k8s_mem": 16.0}

    def test_profiling_results_without_metrics(self):
        """When no metrics server is present, total_cores and total_memory are None."""
        report = KubeReport()
        result = report.profiling_results
        # None values must be excluded so downstream arithmetic doesn't break
        assert "k8s_cores" not in result
        assert "k8s_mem" not in result
        assert result == {}

    def test_profiling_results_partial_metrics(self):
        """Only non-None metrics should appear in the result."""
        report = KubeReport(total_cores=1.0)
        result = report.profiling_results
        assert result == {"k8s_cores": 1.0}
        assert "k8s_mem" not in result

        report = KubeReport(total_memory=8.0)
        result = report.profiling_results
        assert result == {"k8s_mem": 8.0}
        assert "k8s_cores" not in result


# ---------------------------------------------------------------------------
# Issue #504 – Display classes handle None metrics gracefully
# ---------------------------------------------------------------------------


class TestKubeDisplayNoneMetrics:
    """Display update methods must not crash when report fields are None."""

    def test_kube_display_update_with_none_metrics(self):
        """KubeDisplay.update should not raise when total_cores/total_memory are None."""
        from stimela.display.styles.kube import KubeDisplay

        # Create a minimal KubeDisplay by mocking the Progress dependency
        with patch("stimela.display.styles.kube.timer_element") as mock_timer:
            mock_progress = MagicMock()
            mock_progress.columns = [MagicMock(), MagicMock()]
            mock_progress.columns[1].get_table_column.return_value = MagicMock()
            mock_progress.add_task.return_value = 0
            mock_timer.return_value = mock_progress

            display = KubeDisplay(mock_progress)

        # Report with no metrics (simulates missing metrics server)
        report = KubeReport(
            running_pods=1,
            pending_pods=0,
            terminating_pods=0,
            successful_pods=0,
            failed_pods=0,
            stateless_pods=0,
            total_pods=1,
            # total_cores and total_memory intentionally left as None
        )

        task_info = MagicMock()
        task_info.description = "test"
        task_info.status = "running"
        task_info.command = "echo hello"

        # This must not raise TypeError
        display.update(task_info, report)

    def test_simple_kube_display_update_with_none_metrics(self):
        """SimpleKubeDisplay.update should not raise when total_cores/total_memory are None."""
        from stimela.display.styles.kube import SimpleKubeDisplay

        mock_progress = MagicMock()
        mock_progress.columns = [MagicMock(), MagicMock()]
        mock_progress.add_task.return_value = 0

        with patch("stimela.display.styles.kube.timer_element") as mock_timer:
            mock_timer.return_value = mock_progress
            display = SimpleKubeDisplay(mock_progress)

        report = KubeReport()  # All None except connection_status

        task_info = MagicMock()
        task_info.description = "test"
        task_info.command = "echo hello"

        # This must not raise TypeError
        display.update(task_info, report)


# ---------------------------------------------------------------------------
# Issue #301 – PVC deletion handles 404 gracefully
# ---------------------------------------------------------------------------


def _import_infrastructure():
    """Import the infrastructure module, which requires the kube backend to be loadable.

    The kube __init__ tries to load kubernetes config at module level. We need to
    ensure the module is importable even without a real kube config by pre-importing
    stimela.backends.kube first (which gracefully handles missing kubernetes).
    """
    # Ensure the parent kube package is loaded
    import stimela.backends.kube  # noqa: F401

    # Now import the infrastructure submodule
    from stimela.backends.kube import infrastructure

    return infrastructure


try:
    import kubernetes  # noqa: F401

    HAS_KUBERNETES = True
except ImportError:
    HAS_KUBERNETES = False


@pytest.mark.skipif(not HAS_KUBERNETES, reason="kubernetes package not installed")
class TestPvcDeletion404:
    """delete_pvcs should log a warning (not an error) on 404 ApiException."""

    def test_delete_pvc_404_logs_warning(self):
        from kubernetes.client.rest import ApiException

        from stimela.backends.kube import KubeBackendOptions

        infrastructure = _import_infrastructure()
        Lifecycle = KubeBackendOptions.Volume.Lifecycle

        # Set up mock k8s API
        mock_kube_api = MagicMock()

        # Simulate 404 on PVC deletion
        body_404 = json.dumps(
            {
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "message": 'persistentvolumeclaims "test-pvc-abc123" not found',
                "reason": "NotFound",
                "code": 404,
            }
        )
        exc = ApiException(status=404, reason="Not Found")
        exc.body = body_404
        mock_kube_api.delete_namespaced_persistent_volume_claim.side_effect = exc

        # Set up a PVC in active_pvcs
        pvc = KubeBackendOptions.Volume(
            name="test-pvc-abc123",
            capacity="10Gi",
            lifecycle=Lifecycle.step,
        )
        pvc.status = "Bound"
        infrastructure.active_pvcs["test-vol"] = pvc

        kube = MagicMock()
        log = MagicMock()

        try:
            with patch.object(infrastructure, "get_kube_api", return_value=("test-ns", mock_kube_api, MagicMock())):
                with patch.object(infrastructure, "refresh_pvc_list"):
                    infrastructure.delete_pvcs(kube, ["test-vol"], log=log, step=True, refresh=False)

            # Verify warning was logged, not error
            log.warning.assert_called_once()
            warning_msg = log.warning.call_args[0][0]
            assert "not found" in warning_msg.lower() or "404" in warning_msg
        finally:
            # Clean up module-level state
            infrastructure.active_pvcs.pop("test-vol", None)

    def test_delete_pvc_other_error_logs_error(self):
        """Non-404 ApiException during PVC deletion should still log as error."""
        from kubernetes.client.rest import ApiException

        from stimela.backends.kube import KubeBackendOptions

        infrastructure = _import_infrastructure()
        Lifecycle = KubeBackendOptions.Volume.Lifecycle

        mock_kube_api = MagicMock()

        # Simulate 500 error
        body_500 = json.dumps(
            {
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "message": "Internal server error",
                "reason": "InternalError",
                "code": 500,
            }
        )
        exc = ApiException(status=500, reason="Internal Server Error")
        exc.body = body_500
        mock_kube_api.delete_namespaced_persistent_volume_claim.side_effect = exc

        pvc = KubeBackendOptions.Volume(
            name="test-pvc-xyz789",
            capacity="10Gi",
            lifecycle=Lifecycle.step,
        )
        pvc.status = "Bound"
        infrastructure.active_pvcs["test-vol-err"] = pvc

        kube = MagicMock()
        log = MagicMock()

        try:
            with patch.object(infrastructure, "get_kube_api", return_value=("test-ns", mock_kube_api, MagicMock())):
                with patch.object(infrastructure, "refresh_pvc_list"):
                    infrastructure.delete_pvcs(kube, ["test-vol-err"], log=log, step=True, refresh=False)

            # 500 errors should NOT be logged as warnings
            log.warning.assert_not_called()
        finally:
            infrastructure.active_pvcs.pop("test-vol-err", None)
