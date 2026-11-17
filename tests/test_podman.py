"""Tests for the podman backend.

These use a fake podman executable (tests/fake_bin/podman), which logs its invocations and emulates the
image store, and runs the requested command natively. This exercises the full path from the stimela CLI down to
the podman command line, without needing podman itself.
"""

import json
import os
import re

import pytest

from .test_recipe import run, verify_output

FAKE_BIN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_bin")
IMAGE = "quay.io/stimela2/fake-image:latest"


@pytest.fixture
def fake_podman(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", FAKE_BIN_PATH + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_PODMAN_DIR", str(tmp_path))

    def calls():
        with open(tmp_path / "calls.jsonl") as f:
            return [json.loads(line) for line in f]

    def add_image(name):
        with open(tmp_path / "images.txt", "a") as f:
            f.write(name + "\n")

    yield calls, add_image

    if os.path.exists("test-podman-touched.txt"):
        os.unlink("test-podman-touched.txt")


def _option_values(args, option):
    """Returns values given to option, in either "--opt value" or "--opt=value" form"""
    values = []
    for i, arg in enumerate(args):
        if arg == option and i + 1 < len(args):
            values.append(args[i + 1])
        elif arg.startswith(option + "="):
            values.append(arg.split("=", 1)[1])
    return values


def test_podman_run(fake_podman):
    calls, _ = fake_podman
    retcode, output = run("stimela -v run test_podman.yml podman_recipe")
    print(output)
    assert retcode == 0
    assert verify_output(output, "podman image .* does not exist", "pulled " + IMAGE)
    assert verify_output(output, "podman-says-hi")
    assert verify_output(output, "hello-from-env")
    assert os.path.exists("test-podman-touched.txt")

    all_calls = calls()
    pulls = [c for c in all_calls if c[0] == "pull"]
    runs = [c for c in all_calls if c[0] == "run"]
    # image is pulled once, then reused by the subsequent steps
    assert pulls == [["pull", IMAGE]]
    assert len(runs) == 3

    args = runs[0]
    cwd = os.getcwd()
    assert args[0] == "run" and "--rm" in args
    assert _option_values(args, "--pull") == ["missing"]
    assert "--init" in args
    assert _option_values(args, "--userns") == (["keep-id"] if os.geteuid() != 0 else [])
    assert _option_values(args, "--network") == ["host"]
    assert _option_values(args, "--ipc") == ["host"]
    assert _option_values(args, "--workdir") == [cwd]
    assert _option_values(args, "--env") == ["HOME=/tmp", "STIMELA_PODMAN_TEST=hello-from-env"]
    # command runs via --entrypoint, followed by image and arguments
    assert _option_values(args, "--entrypoint") == ["echo"]
    assert args[-2:] == [IMAGE, "podman-says-hi"]

    # conmon pidfile is given (for monitoring), in a temporary directory that is cleaned up afterwards
    pidfiles = _option_values(args, "--conmon-pidfile")
    assert len(pidfiles) == 1
    assert not os.path.exists(os.path.dirname(pidfiles[0]))

    volumes = _option_values(args, "--volume")
    assert f"{cwd}:{cwd}:rw" in volumes
    targets = sorted(vol.split(":")[1] for vol in volumes)
    assert "/tmp" in targets and "/scratch" in targets
    # ephemeral directories are temporary, and should have been cleaned up after the run
    for vol in volumes:
        src, dest, _ = vol.split(":")
        if dest in ("/tmp", "/scratch"):
            assert src != dest
            assert not os.path.exists(src)

    # the output step binds cwd (where the output goes) read-write
    assert f"{cwd}:{cwd}:rw" in _option_values(runs[1], "--volume")
    assert runs[1][-2:] == [IMAGE, "test-podman-touched.txt"]


def test_podman_cli_flag_and_existing_image(fake_podman):
    calls, add_image = fake_podman
    add_image(IMAGE)
    # select native in the config, then override with -P on the command line
    retcode, output = run("stimela -v run -P -c opts.backend.select=native test_podman.yml podman_recipe")
    print(output)
    assert retcode == 0
    assert verify_output(output, "selecting the podman backend")
    assert verify_output(output, f"podman image {IMAGE} exists")
    assert not [c for c in calls() if c[0] == "pull"]


def test_podman_no_auto_pull(fake_podman):
    calls, _ = fake_podman
    retcode, output = run("stimela -v run -c opts.backend.podman.auto_pull=false test_podman.yml podman_recipe")
    print(output)
    assert retcode != 0
    assert verify_output(output, "podman auto_pull not enabled")
    assert not [c for c in calls() if c[0] in ("pull", "run")]


def test_podman_options(fake_podman):
    calls, add_image = fake_podman
    add_image(IMAGE)
    retcode, output = run(
        "stimela -v run test_podman.yml podman_recipe "
        "-c opts.backend.podman.network=null -c opts.backend.podman.ipc=none -c opts.backend.podman.init=false "
        "-c opts.backend.podman.userns=auto -c opts.backend.podman.home=null "
        "-c opts.backend.podman.run_args=[--security-opt,label=disable]"
    )
    print(output)
    assert retcode == 0
    args = [c for c in calls() if c[0] == "run"][0]
    assert "--init" not in args
    assert _option_values(args, "--network") == []
    assert _option_values(args, "--ipc") == ["none"]
    assert _option_values(args, "--env") == ["STIMELA_PODMAN_TEST=hello-from-env"]
    assert _option_values(args, "--security-opt") == ["label=disable"]
    if os.geteuid() != 0:
        assert _option_values(args, "--userns") == ["auto"]


def test_podman_build(fake_podman):
    calls, _ = fake_podman
    retcode, output = run("stimela -v build test_podman.yml podman_recipe")
    print(output)
    assert retcode == 0
    all_calls = calls()
    assert [c for c in all_calls if c[0] == "pull"] == [["pull", IMAGE]]
    assert not [c for c in all_calls if c[0] == "run"]
    assert not os.path.exists("test-podman-touched.txt")


def test_podman_rebuild(fake_podman):
    calls, add_image = fake_podman
    add_image(IMAGE)
    retcode, output = run("stimela -v build -r test_podman.yml podman_recipe")
    print(output)
    assert retcode == 0
    assert verify_output(output, "exists but a re-pull was specified")
    # re-pulled once, even though three steps use the image
    assert [c for c in calls() if c[0] == "pull"] == [["pull", IMAGE]]


def test_podman_pull_failure(fake_podman):
    retcode, output = run("stimela -v run test_podman.yml failpull_recipe")
    print(output)
    assert retcode != 0
    assert verify_output(output, "podman pull returns 125")


def test_podman_prebuilt_image_path(fake_podman):
    retcode, output = run("stimela -v run test_podman.yml prebuilt_recipe")
    print(output)
    assert retcode != 0
    assert verify_output(output, "prebuilt image file")


def test_podman_slurm(fake_podman, tmp_path):
    calls, _ = fake_podman
    retcode, output = run("stimela -v run --slurm test_podman.yml podman_recipe")
    print(output)
    assert retcode == 0
    assert verify_output(output, "podman-says-hi")
    all_calls = calls()
    # no local image checks or pulls when wrapped: the node's podman pulls via "run --pull missing"
    assert not [c for c in all_calls if c[0] in ("image", "pull")]
    runs = [c for c in all_calls if c[0] == "run"]
    assert len(runs) == 3
    # ephemeral binds went to srun as placeholders, and slurm_runner.sh substituted real directories for them
    with open(tmp_path / "srun.jsonl") as f:
        sruns = [json.loads(line) for line in f]
    assert len(sruns) == 3
    assert any(arg.startswith("::EPH") for arg in _option_values(sruns[0], "--volume"))
    for vol in _option_values(runs[0], "--volume"):
        assert "::" not in vol
    # conmon runs on the node, so no local pidfile for monitoring
    assert not _option_values(runs[0], "--conmon-pidfile")
    assert _option_values(runs[0], "--pull") == ["missing"]
    assert sorted(vol.split(":")[1] for vol in _option_values(runs[0], "--volume"))[-2:] == ["/scratch", "/tmp"]


def test_podman_repull_without_auto_pull(fake_podman):
    # an explicit re-pull should pull a missing image even if auto_pull is disabled
    calls, _ = fake_podman
    retcode, output = run(
        "stimela -v run -c opts.backend.podman.auto_pull=false -c opts.backend.podman.repull=true "
        "test_podman.yml podman_recipe"
    )
    print(output)
    assert retcode == 0
    assert [c for c in calls() if c[0] == "pull"] == [["pull", IMAGE]]


def test_podman_slurm_repull(fake_podman):
    # via slurm, a re-pull is delegated to the node as --pull=newer
    calls, _ = fake_podman
    retcode, output = run("stimela -v run --slurm -c opts.backend.podman.repull=true test_podman.yml podman_recipe")
    print(output)
    assert retcode == 0
    runs = [c for c in calls() if c[0] == "run"]
    assert runs and all(_option_values(args, "--pull") == ["newer"] for args in runs)
    assert not [c for c in calls() if c[0] == "pull"]


def test_monitoring_external_pidfile(tmp_path):
    """Processes given by a registered pidfile (and their descendants) are tracked, even if not our descendants"""
    import subprocess
    import time

    from stimela.monitoring import local

    pidfile = tmp_path / "daemon.pid"
    # the shell exits immediately, so the sleep is reparented away from us, like podman's conmon
    subprocess.run(["/bin/sh", "-c", f"sleep 30 >/dev/null 2>&1 & echo $! > {pidfile}"], check=True)
    pid = int(pidfile.read_text())
    try:
        local.update_children()
        assert pid not in local._child_processes
        local.register_external_pidfile(str(pidfile))
        local.update_children()
        assert pid in local._child_processes
        local.unregister_external_pidfile(str(pidfile))
        local.update_children()
        assert pid not in local._child_processes
        # a missing or empty pidfile is quietly ignored
        local.register_external_pidfile(str(tmp_path / "nonexistent.pid"))
        local.update_children()
        local.unregister_external_pidfile(str(tmp_path / "nonexistent.pid"))
    finally:
        os.kill(pid, 15)
        time.sleep(0.1)


def test_podman_full_reference_with_path(fake_podman):
    # a complete image reference is usable, even if the cab also gives a (singularity) image path of the same name
    calls, _ = fake_podman
    retcode, output = run("stimela -v run test_podman.yml fullref_recipe")
    print(output)
    assert retcode == 0
    assert verify_output(output, "fullref-ok")
    assert [c for c in calls() if c[0] == "run"][0][-2:] == [IMAGE, "fullref-ok"]


def test_podman_remote_build_not_deduplicated(fake_podman, tmp_path):
    # with builds going via srun, each pull may land on a different node, so none are skipped as duplicates
    calls, _ = fake_podman
    retcode, output = run(
        "stimela -v build --slurm -c opts.backend.slurm.build_local=false test_podman.yml podman_recipe"
    )
    print(output)
    assert retcode == 0
    with open(tmp_path / "srun.jsonl") as f:
        sruns = [json.loads(line) for line in f]
    assert len(sruns) == 3
    assert all(args[-2:] == ["pull", IMAGE] for args in sruns)
    assert not [c for c in calls() if c[0] == "image"]


def test_podman_availability(monkeypatch, tmp_path):
    """Availability is checked per executable setting, and remote_only isn't spoiled by a cached local failure"""
    from stimela.backends import podman
    from stimela.backends.podman import PodmanBackendOptions

    monkeypatch.setattr(podman, "_detected", {})
    # a PATH with python3 (needed by the fake podman), but no podman
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "python3").symlink_to(os.sys.executable)
    monkeypatch.setenv("PATH", str(bindir))
    monkeypatch.setenv("FAKE_PODMAN_DIR", str(tmp_path))
    assert not podman.is_available(PodmanBackendOptions())
    assert podman.get_status() == "not installed"
    # remote-only is available regardless
    assert podman.is_available(PodmanBackendOptions(remote_only=True))
    # an explicit executable is checked in its own right
    fake = os.path.join(FAKE_BIN_PATH, "podman")
    assert podman.is_available(PodmanBackendOptions(executable=fake))
    assert podman.VERSION == "5.0.0-fake" and podman.BINARY == fake
    # and the PATH lookup result is still remembered
    assert not podman.is_available(PodmanBackendOptions())


def test_podman_name_with_path(fake_podman):
    # an explicit image name is usable (with the default registry and version), even if a path is also given
    calls, _ = fake_podman
    retcode, output = run("stimela -v run test_podman.yml namepath_recipe")
    print(output)
    assert retcode == 0
    assert [c for c in calls() if c[0] == "run"][0][-2:] == [IMAGE, "namepath-ok"]


def test_podman_failed_step_removes_container(fake_podman):
    # a failed (or interrupted) step force-removes its container, in case the workload is still running
    calls, add_image = fake_podman
    add_image(IMAGE)
    retcode, output = run("stimela -v run test_podman.yml fail_recipe")
    print(output)
    assert retcode != 0
    all_calls = calls()
    rms = [c for c in all_calls if c[0] == "rm"]
    assert len(rms) == 1
    assert rms[0][:-1] == ["rm", "--force", "--ignore", "--time", "0"]
    assert rms[0][-1].startswith("fakecid")
    # but a successful step doesn't bother
    retcode, output = run("stimela -v run test_podman.yml podman_recipe")
    assert retcode == 0
    assert len([c for c in calls() if c[0] == "rm"]) == 1


def test_podman_remote_only_executable(fake_podman, tmp_path):
    # a remote-only step must not reuse the podman path found by an earlier local step: the node finds its own
    calls, _ = fake_podman
    retcode, output = run("stimela -v run test_podman.yml mixed_recipe")
    print(output)
    assert retcode == 0
    assert verify_output(output, "local-step", "remote-step")
    with open(tmp_path / "srun.jsonl") as f:
        sruns = [json.loads(line) for line in f]
    assert len(sruns) == 1
    assert sruns[0][sruns[0].index("run") - 1] == "podman"
    # whereas the local step ran the fake podman by its full path
    assert verify_output(output, os.path.join(FAKE_BIN_PATH, "podman") + " run")


def test_podman_not_installed(monkeypatch):
    # no podman on PATH: backend selection must fail with a clear reason
    monkeypatch.setenv("PATH", os.path.dirname(os.sys.executable) + os.pathsep + "/usr/bin:/bin")
    if any(os.path.exists(os.path.join(d, "podman")) for d in ("/usr/bin", "/bin")):
        pytest.skip("real podman installed")
    retcode, output = run("stimela -v run test_podman.yml podman_recipe")
    print(output)
    assert retcode != 0
    assert verify_output(output, "unable to select a backend", "podman: not installed")


REAL_PODMAN_RECIPE = """
cabs:
  shell:
    command: /bin/sh -c
    image: {registry: docker.io/library, name: alpine, version: "3.20"}
    inputs:
      script: {dtype: str, required: true, policies: {positional: true}}
  touchfile:
    command: touch
    image: {registry: docker.io/library, name: alpine, version: "3.20"}
    outputs:
      file: {dtype: File, required: true, must_exist: false, policies: {positional: true}}

opts:
  backend:
    select: podman
    podman:
      env:
        STIMELA_PODMAN_TEST: hello-from-env
      bind_dirs:
        remap:
          host: DATADIR
          target: /remapped
          mode: ro
        scratch:
          host: ephemeral
          target: /scratch

real_recipe:
  steps:
    probe:
      cab: shell
      params:
        script: >-
          echo "UID=$(id -u)"; touch "$HOME/.probe" && echo HOME_WRITABLE; echo "VAR=$STIMELA_PODMAN_TEST";
          cat /remapped/in.txt; touch /remapped/x 2>/dev/null && echo RO_BROKEN || echo RO_ENFORCED;
          touch /scratch/s && echo SCRATCH_WRITABLE; touch /tmp/t && echo TMP_WRITABLE
    touch:
      cab: touchfile
      params:
        file: OUTDIR/touched.txt
"""


def _real_podman():
    """Returns path to a real podman binary, or None"""
    from shutil import which

    path = which("podman")
    if path and os.path.dirname(os.path.abspath(path)) != FAKE_BIN_PATH:
        return path
    return None


@pytest.mark.skipif(_real_podman() is None, reason="podman not installed")
def test_podman_real(tmp_path):
    """Runs a recipe through a real podman (rootless, if not running as root), pulling a small image.

    Note that tmp_path is normally under /tmp, so the working directory gets bound inside the container's /tmp,
    which is itself an ephemeral directory. Rootless podman then creates mountpoints inside the ephemeral
    directory, owned by a subordinate UID, which exercises the "podman unshare" cleanup path.
    """
    workdir, datadir, outdir = tmp_path / "work", tmp_path / "data", tmp_path / "out"
    for d in workdir, datadir, outdir:
        d.mkdir()
    (datadir / "in.txt").write_text("remapped-data-ok\n")
    (workdir / "recipe.yml").write_text(
        REAL_PODMAN_RECIPE.replace("DATADIR", str(datadir)).replace("OUTDIR", str(outdir))
    )

    # -I tmp_path makes for a unique config cache key, in case a stale cache predates the podman backend
    retcode, output = run(f"cd {workdir} && stimela -v -I {tmp_path} run -P recipe.yml real_recipe")
    print(output)
    assert retcode == 0
    if os.geteuid() != 0:
        # --userns=keep-id: same UID inside the container as outside
        assert verify_output(output, f"UID={os.getuid()}")
    assert verify_output(
        output,
        "HOME_WRITABLE",
        "VAR=hello-from-env",
        "remapped-data-ok",
        "RO_ENFORCED",
        "SCRATCH_WRITABLE",
        "TMP_WRITABLE",
    )
    assert not os.path.exists(datadir / "x")
    assert not verify_output(output, "failed to remove temporary directory")
    # output file written via an automatic bind, and owned by us
    touched = outdir / "touched.txt"
    assert touched.exists()
    assert touched.stat().st_uid == os.getuid()
    # temporary directories cleaned up
    for tmpdir in set(re.findall(r"binding (/tmp/tmp\w+) to", output)):
        assert not os.path.exists(tmpdir)
