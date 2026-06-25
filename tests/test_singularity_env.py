"""Tests for environment variable merging across backends (issue #334).

Verifies that cab.management.environment is applied to all backends:
- singularity: merged into --env flags
- native: merged into subprocess env
- kube: merged into kube.env before pod/dask creation
"""

import os

from omegaconf import OmegaConf

from stimela.backends.singularity import SingularityBackendOptions


def _build_env_args(management_env, backend_env):
    """Reproduce the env merging logic from singularity.run() for testing.

    This mirrors lines 324-328 of singularity.py:
        env = dict(cab.management.environment or {})
        env.update(backend.singularity.env or {})
        if env:
            args += ["--env", ",".join([f"{k}={v}" for k, v in env.items()])]
    """
    args = []
    env = dict(management_env or {})
    env.update(backend_env or {})
    if env:
        args += ["--env", ",".join([f"{k}={v}" for k, v in env.items()])]
    return args


def test_management_env_only():
    """cab.management.environment vars should be passed via --env."""
    management_env = {"NUMBA_CACHE_DIR": "/data/numba_cache", "FOO": "bar"}
    backend_env = {}

    args = _build_env_args(management_env, backend_env)

    assert "--env" in args
    env_str = args[args.index("--env") + 1]
    assert "NUMBA_CACHE_DIR=/data/numba_cache" in env_str
    assert "FOO=bar" in env_str


def test_backend_env_only():
    """backend.singularity.env vars should be passed via --env."""
    management_env = {}
    backend_env = {"MY_VAR": "my_value"}

    args = _build_env_args(management_env, backend_env)

    assert "--env" in args
    env_str = args[args.index("--env") + 1]
    assert "MY_VAR=my_value" in env_str


def test_backend_env_takes_precedence():
    """backend.singularity.env should override cab.management.environment for same key."""
    management_env = {"SHARED_VAR": "from_management", "MGMT_ONLY": "yes"}
    backend_env = {"SHARED_VAR": "from_backend", "BACKEND_ONLY": "yes"}

    args = _build_env_args(management_env, backend_env)

    assert "--env" in args
    env_str = args[args.index("--env") + 1]
    # backend value wins for the shared key
    assert "SHARED_VAR=from_backend" in env_str
    assert "SHARED_VAR=from_management" not in env_str
    # unique keys from both sources are present
    assert "MGMT_ONLY=yes" in env_str
    assert "BACKEND_ONLY=yes" in env_str


def test_empty_env_no_flag():
    """No --env flag when both env dicts are empty."""
    args = _build_env_args({}, {})
    assert "--env" not in args


def test_none_env_no_flag():
    """No --env flag when env values are None."""
    args = _build_env_args(None, None)
    assert "--env" not in args


def test_singularity_backend_options_env_default():
    """SingularityBackendOptions.env defaults to an empty dict."""
    opts = SingularityBackendOptions()
    assert opts.env == {} or not opts.env


def test_singularity_backend_options_env_via_omegaconf():
    """Environment variables can be set via OmegaConf merge on SingularityBackendOptions."""
    base = OmegaConf.structured(SingularityBackendOptions)
    override = OmegaConf.create({"env": {"NUMBA_CACHE_DIR": "/cache"}})
    merged = OmegaConf.merge(base, override)

    assert merged.env.NUMBA_CACHE_DIR == "/cache"


# --- Native backend env tests ---


def _build_native_env(management_env):
    """Reproduce the native backend env merging logic from run_native.py."""
    env = None
    if management_env:
        env = os.environ.copy()
        env.update(management_env)
    return env


def test_native_management_env_applied():
    """Native backend should merge cab.management.environment into subprocess env."""
    env = _build_native_env({"MY_VAR": "hello"})
    assert env is not None
    assert env["MY_VAR"] == "hello"
    assert "PATH" in env


def test_native_no_env_returns_none():
    """Native backend should return None env when no management env is set."""
    assert _build_native_env({}) is None
    assert _build_native_env(None) is None


# --- Kube backend env tests ---


def _build_kube_env(management_env, kube_env):
    """Reproduce the kube backend env merging logic from run_kube.py."""
    if management_env:
        merged = dict(management_env)
        merged.update(kube_env or {})
        return merged
    return dict(kube_env or {})


def test_kube_management_env_merged():
    """Kube backend should merge cab.management.environment into kube.env."""
    result = _build_kube_env({"MGMT_VAR": "yes"}, {"KUBE_VAR": "also"})
    assert result["MGMT_VAR"] == "yes"
    assert result["KUBE_VAR"] == "also"


def test_kube_backend_env_takes_precedence():
    """Kube backend env should override cab.management.environment for same key."""
    result = _build_kube_env({"SHARED": "from_mgmt"}, {"SHARED": "from_kube"})
    assert result["SHARED"] == "from_kube"


def test_kube_no_management_env():
    """Kube backend should use only kube.env when no management env."""
    result = _build_kube_env(None, {"KUBE_ONLY": "val"})
    assert result == {"KUBE_ONLY": "val"}
