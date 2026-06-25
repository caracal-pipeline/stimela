"""Tests for graceful degradation when kube metrics server is unavailable."""

import importlib.util
from unittest.mock import MagicMock

import pytest
from rich.progress import Progress

from stimela.monitoring.kube import KubeReport

HAS_KUBERNETES = importlib.util.find_spec("kubernetes") is not None


class TestKubeReportWithoutMetrics:
    """Test that KubeReport handles None metrics values correctly."""

    def test_default_report_has_none_metrics(self):
        report = KubeReport()
        assert report.total_cores is None
        assert report.total_memory is None

    def test_profiling_results_omits_none_values(self):
        report = KubeReport()
        assert report.profiling_results == {}

    def test_profiling_results_includes_cores_when_present(self):
        report = KubeReport(total_cores=2.5)
        result = report.profiling_results
        assert result == {"k8s_cores": 2.5}

    def test_profiling_results_includes_memory_when_present(self):
        report = KubeReport(total_memory=16.0)
        result = report.profiling_results
        assert result == {"k8s_mem": 16.0}

    def test_profiling_results_includes_both_when_present(self):
        report = KubeReport(total_cores=4.0, total_memory=32.0)
        result = report.profiling_results
        assert result == {"k8s_cores": 4.0, "k8s_mem": 32.0}


class TestKubeDisplayWithoutMetrics:
    """Test that kube display classes handle None metrics gracefully."""

    def _make_display(self, cls):
        run_timer = Progress()
        run_timer.add_task("")
        display = cls.__new__(cls)
        display.run_elapsed = run_timer
        display.run_elapsed_id = 0
        display.__init__(run_timer)
        return display

    def test_kube_display_handles_none_metrics(self):
        from stimela.display.styles.kube import KubeDisplay

        display = self._make_display(KubeDisplay)
        report = KubeReport(
            running_pods=1,
            pending_pods=0,
            terminating_pods=0,
            successful_pods=0,
            failed_pods=0,
            stateless_pods=0,
            total_pods=1,
            total_cores=None,
            total_memory=None,
        )
        display.update(None, report)

    def test_kube_display_handles_present_metrics(self):
        from stimela.display.styles.kube import KubeDisplay

        display = self._make_display(KubeDisplay)
        report = KubeReport(
            running_pods=1,
            pending_pods=0,
            terminating_pods=0,
            successful_pods=0,
            failed_pods=0,
            stateless_pods=0,
            total_pods=1,
            total_cores=2.5,
            total_memory=8.0,
        )
        display.update(None, report)

    def test_simple_kube_display_handles_none_metrics(self):
        from stimela.display.styles.kube import SimpleKubeDisplay

        display = self._make_display(SimpleKubeDisplay)
        report = KubeReport(
            running_pods=1,
            pending_pods=0,
            successful_pods=0,
            failed_pods=0,
            total_cores=None,
            total_memory=None,
        )
        display.update(None, report)

    def test_simple_kube_display_handles_present_metrics(self):
        from stimela.display.styles.kube import SimpleKubeDisplay

        display = self._make_display(SimpleKubeDisplay)
        report = KubeReport(
            running_pods=1,
            pending_pods=0,
            successful_pods=0,
            failed_pods=0,
            total_cores=4.0,
            total_memory=16.0,
        )
        display.update(None, report)

    def test_kube_display_non_kube_report_is_noop(self):
        from stimela.display.styles.kube import KubeDisplay

        display = self._make_display(KubeDisplay)
        display.update(None, "not a KubeReport")


@pytest.mark.skipif(not HAS_KUBERNETES, reason="kubernetes package not installed")
class TestStatusReporterMetricsDisable:
    """Test that StatusReporter disables metrics after API failure."""

    def test_metrics_disabled_after_api_error(self):
        from unittest.mock import patch

        from kubernetes.client.rest import ApiException

        with patch("stimela.backends.kube.kube_utils.get_kube_api") as mock_get_kube_api:
            from stimela.backends.kube.kube_utils import StatusReporter

            mock_kube_api = MagicMock()
            mock_custom_api = MagicMock()
            mock_get_kube_api.return_value = ("default", mock_kube_api, mock_custom_api)

            mock_kube_api.list_namespaced_event.return_value = MagicMock(items=[])
            mock_kube_api.list_namespaced_pod.return_value = MagicMock(items=[])
            mock_custom_api.list_namespaced_custom_object.side_effect = ApiException(status=404, reason="Not Found")

            kube = MagicMock()
            kube.debug.log_events = False
            log = MagicMock()

            statrep = StatusReporter(podname="test-pod", log=log, kube=kube, event_handler=None)
            assert statrep.enable_metrics is True

            report = statrep.update()
            assert statrep.enable_metrics is False
            assert report.total_cores is None
            assert report.total_memory is None

            mock_custom_api.list_namespaced_custom_object.reset_mock()
            report = statrep.update()
            mock_custom_api.list_namespaced_custom_object.assert_not_called()

    def test_metrics_not_disabled_on_transient_5xx(self):
        from unittest.mock import patch

        from kubernetes.client.rest import ApiException

        with patch("stimela.backends.kube.kube_utils.get_kube_api") as mock_get_kube_api:
            from stimela.backends.kube.kube_utils import StatusReporter

            mock_kube_api = MagicMock()
            mock_custom_api = MagicMock()
            mock_get_kube_api.return_value = ("default", mock_kube_api, mock_custom_api)

            mock_kube_api.list_namespaced_event.return_value = MagicMock(items=[])
            mock_kube_api.list_namespaced_pod.return_value = MagicMock(items=[])
            mock_custom_api.list_namespaced_custom_object.side_effect = ApiException(
                status=503, reason="Service Unavailable"
            )

            kube = MagicMock()
            kube.debug.log_events = False
            log = MagicMock()

            statrep = StatusReporter(podname="test-pod", log=log, kube=kube, event_handler=None)
            assert statrep.enable_metrics is True

            report = statrep.update()
            assert statrep.enable_metrics is True
            assert report.total_cores is None
            assert report.total_memory is None
