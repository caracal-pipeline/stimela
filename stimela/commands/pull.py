from stimela import logger, LOG_FILE, BASE,  utils
from stimela.backends import docker, singularity, podman


def make_parser(subparsers):

    parser = subparsers.add_parser('pull', help='Pull docker stimela base images')

    add = parser.add_argument

    add("-im", "--image", nargs="+", metavar="IMAGE[:TAG]",
        help="Pull base image along with its tag (or version). Can be called multiple times")

    add("-f", "--force", action="store_true",
        help="force pull if image already exists")

    add("-s", "--singularity", action="store_true",
        help="Pull base images using singularity."
        "Images will be pulled into the directory specified by the enviroment varaible, STIMELA_PULLFOLDER. $PWD by default")

    add("-d", "--docker", action="store_true",
        help="Pull base images using docker.")

    add("-p", "--podman", action="store_true",
        help="Pull base images using podman.")

    add("-cb", "--cab-base", nargs="+",
        help="Pull base image for specified cab")

    add("-pf", "--pull-folder",
        help="Images will be placed in this folder. Else, if the environmnental variable 'STIMELA_PULLFOLDER' is set, then images will be placed there. "
        "Else, images will be placed in the current directory")

    parser.set_defaults(func=pull)


def pull(args, conf):
    pass
