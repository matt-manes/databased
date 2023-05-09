import argshell
from pathier import Pathier

from databased import DataBased, dbparsers


class DBShell(argshell.ArgShell):
    intro = "Starting dbshell (enter help or ? for arg info)..."
    prompt = "based>"
    dbpath: Pathier = None  # type: ignore

    def do_use_db(self, arg: str):
        """Set which database file to use."""
        dbpath = Pathier(arg)
        if not dbpath.exists():
            print(f"{dbpath} does not exist.")
            print(f"Still using {self.dbpath}")
        elif not dbpath.is_file():
            print(f"{dbpath} is not a file.")
            print(f"Still using {self.dbpath}")
        else:
            self.dbpath = dbpath

    def do_dbpath(self, arg: str):
        """Print the .db file in use."""
        print(self.dbpath)

    @argshell.with_parser(dbparsers.get_backup_parser)
    def do_backup(self, args: argshell.Namespace):
        """Create a backup of the current db file."""
        print(f"Creating a back up for {self.dbpath}...")
        backup_path = self.dbpath.backup(args.timestamp)
        print("Creating backup is complete.")
        print(f"Backup path: {backup_path}")

    def do_size(self, arg: str):
        """Display the size of the the current db file."""
        print(f"{self.dbpath.name} is {self.dbpath.size(True)}.")

    @argshell.with_parser(dbparsers.get_create_table_parser)
    def do_add_table(self, args: argshell.Namespace):
        """Add a new table to the database."""
        with DataBased(self.dbpath) as db:
            db.create_table(args.table_name, args.columns)

    def do_drop_table(self, arg: str):
        """Drop the specified table."""
        with DataBased(self.dbpath) as db:
            db.drop_table(arg)

    @argshell.with_parser(
        dbparsers.get_add_row_parser, [dbparsers.verify_matching_length]
    )
    def do_add_row(self, args: argshell.Namespace):
        """Add a row to a table."""
        with DataBased(self.dbpath) as db:
            if db.add_row(args.table_name, args.values, args.columns or None):
                print(f"Added row to {args.table_name} table successfully.")
            else:
                print(f"Failed to add row to {args.table_name} table.")

    @argshell.with_parser(dbparsers.get_info_parser)
    def do_info(self, args: argshell.Namespace):
        """Print out the names of the database tables, their columns, and, optionally, the number of rows."""
        print("Getting database info...")
        with DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            info = [
                {
                    "Table Name": table,
                    "Columns": ", ".join(db.get_column_names(table)),
                    "Number of Rows": db.count(table) if args.rowcount else "n/a",
                }
                for table in tables
            ]
        print(DataBased.data_to_string(info))

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
    def do_show(self, args: argshell.Namespace):
        """Find and print rows from the database.
        Use the -t/--tables, -m/--match_pairs, and -l/--limit flags to limit the search.
        Use the -c/--columns flag to limit what columns are printed.
        Use the -o/--order_by flag to order the results.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs
        Pass -h/--help flag for parser help."""
        print("Finding records... ")
        if len(args.columns) == 0:
            args.columns = None
        with DataBased(self.dbpath) as db:
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
                print(f"{len(results)} matching rows in {table} table:")
                try:
                    print(DataBased.data_to_string(results))  # type: ignore
                except Exception as e:
                    print("Couldn't fit data into a grid.")
                    print(*results, sep="\n")
                print()

    @argshell.with_parser(dbparsers.get_search_parser)
    def do_search(self, args: argshell.Namespace):
        """Search and return any rows containg the searched substring in any of its columns.
        Use the -t/--tables flag to limit the search to a specific table(s).
        Use the -c/--columns flag to limit the search to a specific column(s)."""
        print(f"Searching for {args.search_string}...")
        with DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                columns = args.columns or db.get_column_names(table)
                matcher = " OR ".join(
                    f'{column} LIKE "%{args.search_string}%"' for column in columns
                )
                query = f"SELECT * FROM {table} WHERE {matcher};"
                results = db.query(query)
                results = [db._get_dict(table, result) for result in results]
                print(f"Found {len(results)} results in {table} table.")
                print(DataBased.data_to_string(results))

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
    def do_count(self, args: argshell.Namespace):
        """Print the number of rows in the database.
        Use the -t/--tables flag to limit results to a specific table(s).
        Use the -m/--match_pairs flag to limit the results to rows matching these criteria.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs.
        Pass -h/--help flag for parser help."""
        print("Counting rows...")
        with DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_rows = db.count(table, args.match_pairs, not args.partial_matching)
                print(f"{num_rows} matching rows in {table} table.")

    def do_query(self, arg: str):
        """Execute a query against the current database."""
        print(f"Executing {arg}")
        with DataBased(self.dbpath) as db:
            results = db.query(arg)
        try:
            for result in results:
                print(*result, sep="|-|")
            print(f"{db.cursor.rowcount} affected rows")
        except Exception as e:
            print(f"{type(e).__name__}: {e}")

    @argshell.with_parser(dbparsers.get_update_parser, [dbparsers.convert_match_pairs])
    def do_update(self, args: argshell.Namespace):
        """Update a column to a new value.
        Two required args: the column (-c/--column) to update and the value (-v/--value) to update to.
        Use the -t/--tables flag to limit what tables are updated.
        Use the -m/--match_pairs flag to specify which rows are updated.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs.
        >>> based>update -c username -v big_chungus -t users -m username lil_chungus

        ^will update the username in the users 'table' to 'big_chungus' where the username is currently 'lil_chungus'^"""
        print("Updating rows...")
        with DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_updates = db.update(
                    table,
                    args.column,
                    args.new_value,
                    args.match_pairs,
                    not args.partial_matching,
                )
                print(f"Updated {num_updates} rows in {table} table.")

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
    def do_delete(self, args: argshell.Namespace):
        """Delete rows from the database.
        Use the -t/--tables flag to limit what tables rows are deleted from.
        Use the -m/--match_pairs flag to specify which rows are deleted.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs.
        >>> based>delete -t users -m username chungus -p

        ^will delete all rows in the 'users' table whose username contains 'chungus'^"""
        print("Deleting records...")
        with DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_rows = db.delete(table, args.match_pairs, not args.partial_matching)
                print(f"Deleted {num_rows} rows from {table} table.")

    @argshell.with_parser(dbparsers.get_add_column_parser)
    def do_add_column(self, args: argshell.Namespace):
        """Add a new column to the specified tables."""
        with DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                db.add_column(table, args.column_name, args.type, args.default_value)

    def do_flush_log(self, arg: str):
        """Clear the log file for this database."""
        log_path = self.dbpath.with_name(self.dbpath.stem + "db.log")
        if not log_path.exists():
            print(f"No log file at path {log_path}")
        else:
            print(f"Flushing log...")
            log_path.write_text("")

    def do_scan_dbs(self, arg: str):
        """Scan the current working directory for `*.db` files and display them.

        If the command is entered as `based>scan_dbs r`, the scan will be performed recursively."""
        cwd = Pathier.cwd()
        if arg.strip() == "r":
            dbs = cwd.rglob("*.db")
        else:
            dbs = cwd.glob("*.db")
        for db in dbs:
            print(db.separate(cwd.stem))

    def do_customize(self, arg: str):
        """Generate a template file in the current working directory for creating a custom DBShell class.
        Expects one argument: the name of the custom dbshell.
        This will be used to name the generated file as well as several components in the file content."""
        custom_file = (Pathier.cwd() / arg.replace(" ", "_")).with_suffix(".py")
        if custom_file.exists():
            print(f"Error: {custom_file.name} already exists in this location.")
        else:
            variable_name = "_".join(word for word in arg.lower().split())
            class_name = "".join(word.capitalize() for word in arg.split())
            content = (Pathier(__file__).parent / "customshell.py").read_text()
            content = content.replace("CustomShell", class_name)
            content = content.replace("customshell", variable_name)
            custom_file.write_text(content)

    def do_vacuum(self, arg: str):
        """Reduce database disk memory."""
        starting_size = self.dbpath.size()
        print(f"Database size before vacuuming: {self.dbpath.size(True)}")
        print("Vacuuming database...")
        with DataBased(self.dbpath) as db:
            db.vacuum()
        print(f"Database size after vacuuming: {self.dbpath.size(True)}")
        print(f"Freed up {Pathier.format_size(starting_size - self.dbpath.size())} of disk space.")  # type: ignore

    def _choose_db(self, options: list[Pathier]) -> Pathier:
        """Prompt the user to select from a list of files."""
        cwd = Pathier.cwd()
        paths = [path.separate(cwd.stem) for path in options]
        while True:
            print(
                f"DB options:\n{' '.join([f'({i}) {path}' for i,path in enumerate(paths,1)])}"
            )
            choice = input("Enter the number of the option to use: ")
            try:
                index = int(choice)
                if not 1 <= index <= len(options):
                    print("Choice out of range.")
                    continue
                return options[index - 1]
            except Exception as e:
                print(f"{choice} is not a valid option.")

    def preloop(self):
        """Scan the current directory for a .db file to use.
        If not found, prompt the user for one or to try again recursively."""
        if self.dbpath:
            self.dbpath = Pathier(self.dbpath)
            print(f"Defaulting to database {self.dbpath}")
        else:
            print("Searching for database...")
            cwd = Pathier.cwd()
            dbs = list(cwd.glob("*.db"))
            if len(dbs) == 1:
                self.dbpath = dbs[0]
                print(f"Using database {self.dbpath}.")
            elif dbs:
                self.dbpath = self._choose_db(dbs)
            else:
                print(f"Could not find a .db file in {cwd}.")
                path = input(
                    "Enter path to .db file to use or press enter to search again recursively: "
                )
                if path:
                    self.dbpath = Pathier(path)
                elif not path:
                    print("Searching recursively...")
                    dbs = list(cwd.rglob("*.db"))
                    if len(dbs) == 1:
                        self.dbpath = dbs[0]
                        print(f"Using database {self.dbpath}.")
                    elif dbs:
                        self.dbpath = self._choose_db(dbs)
                    else:
                        print("Could not find a .db file.")
                        self.dbpath = Pathier(input("Enter path to a .db file: "))
        if not self.dbpath.exists():
            raise FileNotFoundError(f"{self.dbpath} does not exist.")
        if not self.dbpath.is_file():
            raise ValueError(f"{self.dbpath} is not a file.")


def main():
    DBShell().cmdloop()
