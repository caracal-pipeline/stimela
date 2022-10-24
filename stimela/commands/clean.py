from stimela import logger, LOG_FILE, LOG_HOME, utils, CAB_USERNAME


def make_parser(subparsers):
    parser = subparsers.add_parser("clean", help='Convience tools for cleaning up after stimela')

    add = parser.add_argument

    add("-ai", "--all-images", action="store_true",
        help="Remove all images pulled/built by stimela. This include CAB images")

    add("-ab", "--all-base", action="store_true",
        help="Remove all base images")

    add("-ac", "--all-cabs", action="store_true",
        help="Remove all CAB images")

    add("-aC", "--all-containers", action="store_true",
        help="Stop and/or Remove all stimela containers")

    add("-bl", "--build-label", default=CAB_USERNAME,
        help="Label for cab images. All cab images will be named <CAB_LABEL>_<cab name>. The default is $USER")

    parser.set_defaults(func=clean)


def clean(args, conf):
    pass
