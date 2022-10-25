from stimela.main import get_cab_definition
from stimela import logger, LOG_HOME, CAB_USERNAME, CAB_PATH


def make_parser(subparsers):
    parser = subparsers.add_parser("cabs", help='List executor (a.k.a cab) images')

    parser.add_argument("-l", "--list", action="store_true",
                        help="List cab names")

    parser.add_argument("-i", "--cab-doc",
                        help="Will display document about the specified cab. For example, \
to get help on the 'cleanmask cab' run 'stimela cabs --cab-doc cleanmask'")

    parser.add_argument("-ls", "--list-summary", action="store_true",
                        help="List cabs with a summary of the cab")

    parser.set_defaults(func=cabs)


def cabs(args, conf):
    pass


# ## singularity:
# # singularity inspect -l --json ubuntu.img

# ## podman:
# # 