from argshell import ArgShellParser, Namespace, with_parser
from pathier import Pathier

from databased import DataBased, DBShell, dbparsers

root = Pathier(__file__).parent


class CustomShell(DBShell):
    intro = "Starting customshell (enter help or ? for command info)..."
    prompt = "customshell>"
    dbpath: Pathier = None  # Replace None with a path to a .db file to set a default database # type: ignore


# For help with adding custom functionality see:
# https://github.com/matt-manes/argshell
# https://github.com/matt-manes/databased/blob/main/src/databased/dbshell.py
# https://github.com/matt-manes/databased/blob/main/src/databased/dbparsers.py

if __name__ == "__main__":
    CustomShell().cmdloop()