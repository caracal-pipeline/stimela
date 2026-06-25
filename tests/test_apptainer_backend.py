"""Tests for the apptainer backend alias and singularity deprecation."""

import warnings

from stimela.backends import (
    SUPPORTED_BACKENDS,
    StimelaBackendOptions,
    get_backend_status,
)
from stimela.backends.singularity import SingularityBackendOptions


def test_apptainer_in_supported_backends():
    """Apptainer should be listed as a supported backend."""
    assert "apptainer" in SUPPORTED_BACKENDS


def test_singularity_still_in_supported_backends():
    """Singularity should still be a supported backend for backward compatibility."""
    assert "singularity" in SUPPORTED_BACKENDS


def test_default_select_prefers_apptainer():
    """Default backend selection should prefer apptainer over singularity."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        opts = StimelaBackendOptions()
    assert opts.select[0] == "apptainer"
    assert "native" in opts.select


def test_singularity_select_emits_deprecation_warning():
    """Selecting 'singularity' backend should emit a FutureWarning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        StimelaBackendOptions(select="singularity")  # noqa: F841
        future_warnings = [x for x in w if issubclass(x.category, FutureWarning)]
        assert len(future_warnings) >= 1
        assert "deprecated" in str(future_warnings[0].message).lower()
        assert "apptainer" in str(future_warnings[0].message).lower()


def test_apptainer_select_no_warning():
    """Selecting 'apptainer' backend should not emit a deprecation warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        StimelaBackendOptions(select="apptainer")  # noqa: F841
        future_warnings = [x for x in w if issubclass(x.category, FutureWarning)]
        assert len(future_warnings) == 0


def test_apptainer_and_singularity_options_synced():
    """The apptainer and singularity options should point to the same object."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        opts = StimelaBackendOptions()
    # They should be the same object (or at least equivalent)
    assert opts.apptainer is not None
    assert opts.singularity is not None
    assert opts.apptainer is opts.singularity


def test_apptainer_backend_resolves_to_singularity_module():
    """The 'apptainer' backend should resolve to the singularity backend module."""
    from stimela.backends import _resolve_backend_module_name

    assert _resolve_backend_module_name("apptainer") == "singularity"
    assert _resolve_backend_module_name("singularity") == "singularity"
    assert _resolve_backend_module_name("native") == "native"
    assert _resolve_backend_module_name("kube") == "kube"


def test_get_backend_status_apptainer():
    """get_backend_status('apptainer') should return a valid status string."""
    status = get_backend_status("apptainer")
    # Should be either a version string or "not installed", not "unknown backend"
    assert status != "unknown backend"


def test_singularity_options_backward_compat():
    """Setting singularity options should also affect apptainer options."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        sing_opts = SingularityBackendOptions(auto_build=False)
        opts = StimelaBackendOptions(singularity=sing_opts)
    assert opts.apptainer.auto_build is False
    assert opts.singularity.auto_build is False


def test_apptainer_options_set_singularity():
    """Setting apptainer options should also set singularity options."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        app_opts = SingularityBackendOptions(contain=False)
        opts = StimelaBackendOptions(apptainer=app_opts)
    assert opts.apptainer.contain is False
    assert opts.singularity.contain is False
