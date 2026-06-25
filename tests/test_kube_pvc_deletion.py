"""Tests for graceful PVC deletion error handling in the kube backend (issue #301)."""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("kubernetes")

from kubernetes.client.rest import ApiException

from stimela.backends.kube import KubeBackendOptions
from stimela.backends.kube import infrastructure as kube_infra
from stimela.backends.kube.infrastructure import Lifecycle, active_pvcs, delete_pvcs


def _make_api_exception(status, reason):
    exc = ApiException(status=status)
    exc.body = json.dumps({"reason": reason, "message": f"PVC {reason.lower()}", "code": status})
    return exc


def _setup_active_pvc(name, lifecycle=Lifecycle.step):
    pvc = KubeBackendOptions.Volume(name=name, capacity="10Gi", lifecycle=lifecycle)
    pvc.status = "Bound"
    active_pvcs[name] = pvc
    return pvc


@pytest.fixture(autouse=True)
def _clean_active_pvcs():
    active_pvcs.clear()
    kube_infra.terminating_pvcs = {}
    yield
    active_pvcs.clear()
    kube_infra.terminating_pvcs = {}


@patch("stimela.backends.kube.infrastructure.get_kube_api")
@patch("stimela.backends.kube.infrastructure.refresh_pvc_list")
def test_delete_pvc_not_found_is_graceful(mock_refresh, mock_get_api):
    """A 404 NotFound during PVC deletion should log info, not error (issue #301)."""
    mock_kube_api = MagicMock()
    mock_get_api.return_value = ("test-ns", mock_kube_api, MagicMock())
    mock_kube_api.delete_namespaced_persistent_volume_claim.side_effect = _make_api_exception(404, "NotFound")

    kube = KubeBackendOptions()
    _setup_active_pvc("test-pvc")
    log = logging.getLogger("test.kube.pvc")

    with patch.object(log, "info") as mock_info, patch.object(log, "error") as mock_error:
        delete_pvcs(kube, ["test-pvc"], log=log, step=True, refresh=False)

    mock_info.assert_any_call("PVC 'test-pvc' not found, it may have already been deleted")
    mock_error.assert_not_called()


@patch("stimela.backends.kube.infrastructure.get_kube_api")
@patch("stimela.backends.kube.infrastructure.refresh_pvc_list")
def test_delete_pvc_other_api_error_still_logged(mock_refresh, mock_get_api):
    """Non-404 API errors during PVC deletion should still be logged as errors."""
    mock_kube_api = MagicMock()
    mock_get_api.return_value = ("test-ns", mock_kube_api, MagicMock())
    mock_kube_api.delete_namespaced_persistent_volume_claim.side_effect = _make_api_exception(403, "Forbidden")

    kube = KubeBackendOptions()
    _setup_active_pvc("test-pvc")
    log = logging.getLogger("test.kube.pvc")

    with patch("stimela.backends.kube.infrastructure.log_exception") as mock_log_exc:
        delete_pvcs(kube, ["test-pvc"], log=log, step=True, refresh=False)

    mock_log_exc.assert_called_once()
    call_kwargs = mock_log_exc.call_args
    assert call_kwargs[1]["severity"] == "error"


@patch("stimela.backends.kube.infrastructure.get_kube_api")
@patch("stimela.backends.kube.infrastructure.refresh_pvc_list")
def test_delete_pvc_success(mock_refresh, mock_get_api):
    """Successful PVC deletion should mark the PVC as Terminating."""
    mock_kube_api = MagicMock()
    mock_get_api.return_value = ("test-ns", mock_kube_api, MagicMock())
    mock_kube_api.delete_namespaced_persistent_volume_claim.return_value = MagicMock()

    kube = KubeBackendOptions()
    pvc = _setup_active_pvc("test-pvc")
    log = logging.getLogger("test.kube.pvc")

    delete_pvcs(kube, ["test-pvc"], log=log, step=True, refresh=False)

    assert pvc.status == "Terminating"
