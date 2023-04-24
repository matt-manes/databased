import argshell
from pathier import Pathier

import databased
from databased import dbparsers


class DBManager(argshell.ArgShell):
    intro = "Starting dbmanager (enter help or ? for command info)..."
    prompt = "based>"
    dbpath = None

    def do_use_db(self, command: str):
        """Set which database file to use."""
        dbpath = Pathier(command)
        if not dbpath.exists():
            print(f"{dbpath} does not exist.")
            print(f"Still using {self.dbpath}")
        elif not dbpath.is_file():
            print(f"{dbpath} is not a file.")
            print(f"Still using {self.dbpath}")
        else:
            self.dbpath = dbpath

    def do_dbpath(self, command: str):
        """Print the .db file in use."""
        print(self.dbpath)

    def do_backup(self, command: str):
        """Create a backup of the current db file."""
        print(f"Creating a back up for {self.dbpath}...")
        backup_path = self.dbpath.with_stem(f"{self.dbpath.stem}_bckup")
        self.dbpath.copy(backup_path, True)
        print("Creating backup is complete.")
        print(f"Backup path: {backup_path}")

    def do_size(self, command: str):
        """Display the size of the the current db file."""
        print(f"{self.dbpath.name} is {self.dbpath.size(True)}.")

    def do_info(self, command: str):
        """Print out the names of the database tables, their columns, and the number of rows.
        Pass a space-separated list of table names to only print info for those specific tables,
        otherwise all tables will be printed."""
        print("Getting database info...")
        with databased.DataBased(self.dbpath) as db:
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

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
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
        with databased.DataBased(self.dbpath) as db:
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
                    print(databased.data_to_string(results))
                except Exception as e:
                    print("Couldn't fit data into a grid.")
                    print(*results, sep="\n")
                print()

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
    def do_count(self, args: argshell.Namespace):
        """Print the number of rows in the database.
        Use the -t/--tables flag to limit results to a specific table(s).
        Use the -m/--match_pairs flag to limit the results to rows matching these criteria.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs.
        Pass -h/--help flag for parser help."""
        print("Counting rows...")
        with databased.DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_rows = db.count(table, args.match_pairs, not args.partial_matching)
                print(f"{num_rows} matching rows in {table} table.")

    def do_query(self, command: str):
        """Execute a query against the current database."""
        print(f"Executing {command}")
        with databased.DataBased(self.dbpath) as db:
            results = db.query(command)
        try:
            for result in results:
                print(*result, sep="|-|")
        except Exception as e:
            print(f"{type(e).__name__}: {e}")

    @argshell.with_parser(dbparsers.get_update_parser, [dbparsers.convert_match_pairs])
    def do_update(self, args: argshell.Namespace):
        """Update a column to a new value.
        Two required positional args: the column to update and the value to update to.
        Use the -t/--tables flag to limit what tables are updated.
        Use the -m/--match_pairs flag to specify which rows are updated.
        >>> based>update username big_chungus -t users -m username lil_chungus

        ^will update the username in the users 'table' to 'big_chungus' where the username is currently 'lil_chungus'^"""
        print("Updating rows...")
        with databased.DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                if db.update(table, args.column, args.new_value, args.match_pairs):
                    print(f"Updating rows in {table} table successful.")
                else:
                    print(f"Failed to update rows in {table} table.")

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
    def do_delete(self, args: argshell.Namespace):
        """Delete rows from the database.
        Use the -t/--tables flag to limit what tables rows are deleted from.
        Use the -m/--match_pairs flag to specify which rows are deleted.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs.
        >>> based>delete -t users -m username chungus -p

        ^will delete all rows in the 'users' table whose username contains 'chungus'^"""
        print("Deleting records...")
        with databased.DataBased(self.dbpath) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_rows = db.delete(table, args.match_pairs, not args.partial_matching)
                print(f"Deleted {num_rows} rows from {table} table.")

    def do_customize(self, command: str):
        """Generate a template file in the current working directory for creating a custom DBManager class.
        Expects one argument: the name of the custom dbmanager.
        This will be used to name the generated file as well as several components in the file content."""
        custom_file = (Pathier.cwd() / command).with_suffix(".py")
        if custom_file.exists():
            print(f"Error: {custom_file.name} already exists in this location.")
        else:
            variable_name = "_".join(word for word in command.lower().split())
            class_name = "".join(word.capitalize() for word in command.split())
            content = (Pathier(__file__).parent / "custom_manager.py").read_text()
            content = content.replace("CustomManager", class_name)
            content = content.replace("custommanager", variable_name)
            custom_file.write_text(content)

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
            print(f"Defaulting to database {self.dbpath.separate(Pathier.cwd().stem)}")
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
    DBManager().cmdloop()
