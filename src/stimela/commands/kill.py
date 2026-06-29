def make_parser(subparsers):
    parser = subparsers.add_parser("kill", help="Gracefully kill stimela process(s).")

    add = parser.add_argument

    add("pid", nargs="*", help="Process ID")

    parser.set_defaults(func=kill)


def kill(args, conf):
    pass
