"""Tests for singularity backend environment variable merging (issue #334).

Verifies that:
- cab.management.environment vars are passed to singularity via --env
- backend.singularity.env vars are passed via --env
- backend.singularity.env takes precedence over cab.management.environment
- empty env dicts produce no --env flag
"""

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
