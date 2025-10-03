from stimela import LOG_FILE, logger


def make_parser(subparsers):
    parser = subparsers.add_parser("containers", help="List all active stimela containers.")

    add = parser.add_argument

    add(
        "-c",
        "--clear",
        action="store_true",
        help="Clear the log file that keeps track of stimela containers. This doesn't do anything to the containers.",
    )

    parser.set_defaults(func=containers)


def containers(args, conf):
    log = logger.StimelaLogger(LOG_FILE)
    log.display("containers")
    if args.clear:
        log.clear("containers")
        log.write()
