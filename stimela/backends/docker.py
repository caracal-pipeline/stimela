import subprocess
from shutil import which

STATUS = VERSION = BINARY = None

# can only execute cabs that specify a container image
requires_container_image = True


def is_available(opts=None):
    global STATUS, VERSION, BINARY
    if STATUS is None:
        BINARY = which("docker")
        if BINARY:
            __version_string = subprocess.check_output([BINARY, "--version"]).decode("utf8")
            STATUS = VERSION = __version_string.strip().split()[-1]
        else:
            STATUS = "not installed"
            VERSION = None
            BINARY = None
    return False
    # return VERSION is not None


def get_status():
    return "not implemented"
    # is_available()
    # return STATUS


def is_remote():
    return False
