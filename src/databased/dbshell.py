import databased

# from cmd2 import Cmd2ArgumentParser, with_argparser
# import cmd2
import cmd
import argparse
import shlex
from functools import wraps
from typing import Callable, Any
from pathier import Pathier

root = Pathier(__file__).parent


def with_argparse(parser: Callable) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def inner(self, command: str) -> Any:
            command = parser(command)
            return func(self, command)

        return inner

    return decorator


def get_args(command: str) -> argparse.Namespace:
    command = shlex.split(command)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--tables",
        type=str,
        nargs="*",
        default=[],
        help="""Limits commands to a specific list of tables.
        Optional for some commands, required for others.
        If this is the only arg given (besides -db if not already set),
        the whole table will be printed to the terminal.""",
    )
    parser.add_argument(
        "-c",
        "--columns",
        type=str,
        nargs="*",
        default=[],
        help=""" Limits commands to a specific list of columns.
        Optional for some commands, required for others.
        If this and -t are the only args given 
        (besides -db if not already set), the whole table will be printed
        to the terminal, but with only the columns provided with this arg.""",
    )
    parser.add_argument(
        "-m",
        "--match_pairs",
        type=str,
        nargs="*",
        default=[],
        help=""" Pairs of columns and values to use row operations.
        Wildcards ("*") supported (will be)
        i.e. 'find -t users -m name Bob state Alaska last_login *' will print
        all rows from the users table that have the name Bob,
        are from the state Alaska, and last logged in at any date.""",
    )
    args = parser.parse_args(command)
    if args.match_pairs:
        # Create a list of tuples from the pairs
        args.match_pairs = [
            (column, value)
            for column, value in zip(args.match_pairs[::2], args.match_pairs[1::2])
        ]
    return args


class DbShell(cmd.Cmd):
    intro = "Cmd shell testing"
    prompt = "yeet>>>"

    def __init__(self, *args, **kwargs):
        self.dbname = (root - 2) / "tests/test.db"
        super().__init__()

    def do_use_db(self, command: str):
        """Set which database file to use."""
        self.dbname = Pathier(command)

    def do_dbname(self, command: str):
        """Print the .db file in use."""
        print(self.dbname)

    @with_argparse(get_args)
    def do_info(self, command: argparse.Namespace):
        """Print out the names of the database tables, their columns, and the number of rows. Use the -t/--table flag to only show the info for certain tables."""
        print("Getting database info...")
        with databased.DataBased(self.dbname) as db:
            tables = command.tables or db.get_table_names()
            info = [
                {
                    "Table Name": table,
                    "Columns": ", ".join(db.get_column_names(table)),
                    "Number of Rows": db.count(table),
                }
                for table in tables
            ]
        print(databased.data_to_string(info))

    @with_argparse(get_args)
    def do_find(self, command: argparse.Namespace):
        """Find and print rows from the database. Use the -t/--table and -m/--match_pairs flags to limit the search. Use the -c/--columns flag to limit what columns are printed."""
        print("Finding records... ")
        if len(command.columns) == 0:
            command.columns = None
        with databased.DataBased(self.dbname) as db:
            tables = command.tables or db.get_table_names()
            for table in tables:
                results = db.get_rows(
                    table, command.match_pairs, columns_to_return=command.columns
                )
                db.close()
                print(f"{len(results)} matching rows in {table}:")
                try:
                    print(databased.data_to_string(results))
                except Exception as e:
                    print("Couldn't fit data into a grid.")
                    print(*results, sep="\n")
                print()

    def do_quit(self, command: str):
        """Quit shell"""
        return True

    def preloop(self):
        if not self.dbname:
            print("Searching for database...")
            dbs = list(Pathier.cwd().glob("*.db"))
            if dbs:
                self.dbname = dbs[0]
                print(f"Defaulting to {self.dbname}")
            else:
                print(f"Could not find a .db file in {Pathier.cwd()}")
                self.dbname = Pathier(input("Enter path to .db file to use: "))


if __name__ == "__main__":
    DbShell().cmdloop()
