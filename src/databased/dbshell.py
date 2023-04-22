import argshell
from pathier import Pathier

import databased

root = Pathier(__file__).parent


def get_parser() -> argshell.ArgShellParser:
    parser = argshell.ArgShellParser()
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
    parser.add_argument(
        "-o",
        "--order_by",
        type=str,
        default=None,
        help=""" The name of a column to sort results by.
        Can include 'desc' as part of the argument.""",
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=None,
        help=""" Only return this many results. """,
    )
    parser.add_argument(
        "-p",
        "--partial_matching",
        action="store_true",
        help=""" When selecting rows using a string, the string can be a substring instead of an exact match.
        i.e. "-t names -m first theo" only returns rows from names where the first name is exactly 'theo'.
        "-t names -m first theo -p" would return rows with first names of 'theo', but also rows with names like 'theodore'.  """,
    )
    return parser


def get_update_parser() -> argshell.ArgShellParser:
    parser = get_parser()
    parser.add_argument("new_value", help=""" The new value to update to. """)
    return parser


def convert_match_pairs(args: argshell.Namespace) -> argshell.Namespace:
    """Create a list of tuples from match_pairs."""
    if args.match_pairs:
        args.match_pairs = [
            (col, val)
            for col, val in zip(args.match_pairs[::2], args.match_pairs[1::2])
        ]
    return args


class DBManager(argshell.ArgShell):
    intro = "Starting dbmanager..."
    prompt = "based>"
    root = Pathier(__file__).parent
    dbname = (root - 2) / "tests/test.db"  # This is for testing

    def do_use_db(self, command: str):
        """Set which database file to use."""
        self.dbname = Pathier(command)

    def do_dbname(self, command: str):
        """Print the .db file in use."""
        print(self.dbname)

    def do_backup(self, command: str):
        """Create a backup of the current db file."""
        print(f"Creating a back up for {self.dbname}...")
        backup_path = self.dbname.with_stem(f"{self.dbname.stem}_bckup")
        self.dbname.copy(backup_path, True)
        print("Creating backup is complete.")
        print(f"Backup path: {backup_path}")

    def do_info(self, command: str):
        """Print out the names of the database tables, their columns, and the number of rows.
        Pass a space-separated list of table names to only print info for those specific tables,
        otherwise all tables will be printed."""
        print("Getting database info...")
        with databased.DataBased(self.dbname) as db:
            tables = command.split() or db.get_table_names()
            info = [
                {
                    "Table Name": table,
                    "Columns": ", ".join(db.get_column_names(table)),
                    "Number of Rows": db.count(table),
                }
                for table in tables
            ]
        print(databased.data_to_string(info))

    @argshell.with_parser(get_parser, [convert_match_pairs])
    def do_find(self, args: argshell.Namespace):
        """Find and print rows from the database.
        Use the -t/--tables, -m/--match_pairs, and -l/--limit flags to limit the search.
        Use the -c/--columns flag to limit what columns are printed.
        Use the -o/--order_by flag to order the results.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs
        Pass -h/--help flag for parser help."""
        print("Finding records... ")
        if len(args.columns) == 0:
            args.columns = None
        with databased.DataBased(self.dbname) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                results = db.get_rows(
                    table,
                    args.match_pairs,
                    columns_to_return=args.columns,
                    order_by=args.order_by,
                    limit=args.limit,
                    exact_match=not args.partial_matching,
                )
                db.close()
                print(f"{len(results)} matching rows in {table}:")
                try:
                    print(databased.data_to_string(results))
                except Exception as e:
                    print("Couldn't fit data into a grid.")
                    print(*results, sep="\n")
                print()

    @argshell.with_parser(get_parser, [convert_match_pairs])
    def do_count(self, args: argshell.Namespace):
        """Print the number of rows in the database.
        Use the -t/--tables flag to limit results to a specific table(s).
        Use the -m/--match_pairs flag to limit the results to rows matching these criteria.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs
        Pass -h/--help flag for parser help."""
        print("Counting rows...")
        with databased.DataBased(self.dbname) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_rows = db.count(table, args.match_pairs, not args.partial_matching)
                print(f"{num_rows} matching rows in {table}.")

    def do_query(self, command: str):
        """Execute a query against the current database."""
        print(f"Executing {command}")
        with databased.DataBased(self.dbname) as db:
            results = db.query(command)
        try:
            for result in results:
                print(*result, sep="|-|")
        except Exception as e:
            print(f"{type(e).__name__}: {e}")

    @argshell.with_parser(get_update_parser, [convert_match_pairs])
    def do_update(self, args: argshell.Namespace):
        """Update a column to a new value.
        The value to update to is a positional arg.
        Use the -t/--tables flag to specify which tables to update.
        Use the -c/--columns flag to specify the column to update.
        Use the -m/--match_pairs flag to specify what rows to update."""
        print("Updating rows...")
        with databased.DataBased(self.dbname) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                if db.update(table, args.columns[0], args.new_value, args.match_pairs):
                    print(f"Updating rows in {table} table successful.")
                else:
                    print(f"Failed to update rows in {table} table.")

    def preloop(self):
        """Scan the current directory for a .db file to use.
        If not found, prompt the user for one."""
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
    DBManager().cmdloop()
