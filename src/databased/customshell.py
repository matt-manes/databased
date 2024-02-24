from argshell import ArgShellParser, ArgumentParser, Namespace, with_parser
from pathier import Pathier

from databased import Databased, DBShell, dbparsers


class CustomShell(DBShell):
    _dbpath: Pathier = (
        None  # Replace None with a path to a database file to set a default database # type: ignore
    )
    connection_timeout: float = 10
    detect_types: bool = True
    enforce_foreign_keys: bool = True
    commit_on_close: bool = True
    log_dir: Pathier = Pathier(__file__).parent
    intro = "Starting customshell (enter help or ? for command info)..."
    prompt = "customshell>"


# For help with adding custom functionality see:
# https://github.com/matt-manes/argshell
# https://github.com/matt-manes/databased/blob/main/src/databased/dbshell.py
# https://github.com/matt-manes/databased/blob/main/src/databased/dbparsers.py


def get_args() -> Namespace:
    parser = ArgumentParser()

    parser.add_argument(
        "dbpath",
        nargs="?",
        type=str,
        help=""" The database file to use. If not provided the current working directory will be scanned for database files. """,
    )
    args = parser.parse_args()

    return args


def main(args: Namespace | None = None):
    if not args:
        args = get_args()
    dbshell = CustomShell()
    if args.dbpath:
        dbshell.dbpath = Pathier(args.dbpath)
    dbshell.cmdloop()


if __name__ == "__main__":
    main(get_args())
