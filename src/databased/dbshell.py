import argshell
from griddle import griddy
from pathier import Pathier

from databased import Databased, dbparsers
from databased.create_shell import create_shell


class DBShell(argshell.ArgShell):
    dbpath: Pathier = None  # type: ignore
    intro = "Starting dbshell (enter help or ? for arg info)...\nUnrecognized commands will be executed as queries.\nPrepend query with a _ to force unrecognized behavior for commands like `select` or use the `query` command explicitly."
    prompt = "based>"

    def default(self, line: str):
        line = line.strip("_")
        with Databased(self.dbpath) as db:
            self.display(db.query(line))

    def display(self, data: list[dict]):
        """Print row data to terminal in a grid."""
        try:
            print(griddy(data, "keys"))
        except Exception as e:
            print("Could not fit data into grid :(")
            print(e)

    # Seat

    @argshell.with_parser(dbparsers.get_add_column_parser)
    def do_add_column(self, args: argshell.Namespace):
        """Add a new column to the specified tables."""
        with Databased(self.dbpath) as db:
            db.add_column(args.table, args.column_def)

    @argshell.with_parser(dbparsers.get_backup_parser)
    def do_backup(self, args: argshell.Namespace):
        """Create a backup of the current db file."""
        print(f"Creating a back up for {self.dbpath}...")
        backup_path = self.dbpath.backup(args.timestamp)
        print("Creating backup is complete.")
        print(f"Backup path: {backup_path}")

    def do_customize(self, name: str):
        """Generate a template file in the current working directory for creating a custom DBShell class.
        Expects one argument: the name of the custom dbshell.
        This will be used to name the generated file as well as several components in the file content."""
        try:
            create_shell(name)
        except Exception as e:
            print(f"{type(e).__name__}: {e}")

    def do_dbpath(self, _: str):
        """Print the .db file in use."""
        print(self.dbpath)

    @argshell.with_parser(dbparsers.get_delete_parser)
    def do_delete(self, args: argshell.Namespace):
        """Delete rows from the database.

        Syntax:
        >>> delete {table} {where}
        >>> based>delete users "username LIKE '%chungus%"

        ^will delete all rows in the 'users' table whose username contains 'chungus'^"""
        print("Deleting records...")
        with Databased(self.dbpath) as db:
            num_rows = db.delete(args.table, args.where)
            print(f"Deleted {num_rows} rows from {args.table} table.")

    @argshell.with_parser(dbparsers.get_drop_column_parser)
    def do_drop_column(self, args: argshell.Namespace):
        """Drop the specified column from the specified table."""
        with Databased(self.dbpath) as db:
            db.drop_column(args.table, args.column)

    def do_drop_table(self, table: str):
        """Drop the specified table."""
        with Databased(self.dbpath) as db:
            db.drop_table(table)

    def do_flush_log(self, _: str):
        """Clear the log file for this database."""
        log_path = self.dbpath.with_name(self.dbpath.stem + "db.log")
        if not log_path.exists():
            print(f"No log file at path {log_path}")
        else:
            print(f"Flushing log...")
            log_path.write_text("")

    @argshell.with_parser(dbparsers.get_info_parser)
    def do_info(self, args: argshell.Namespace):
        """Print out the names of the database tables, their columns, and, optionally, the number of rows."""
        print("Getting database info...")
        with Databased(self.dbpath) as db:
            tables = args.tables or db.tables
            info = [
                {
                    "Table Name": table,
                    "Columns": ", ".join(db.get_columns(table)),
                    "Number of Rows": db.count(table) if args.rowcount else "n/a",
                }
                for table in tables
            ]
        self.display(info)

    def do_query(self, query: str):
        """Execute a query against the current database."""
        print(f"Executing {query}")
        with Databased(self.dbpath) as db:
            results = db.query(query)
        self.display(results)
        print(f"{db.cursor.rowcount} affected rows")

    def do_restore(self, file: str):
        """Replace the current db file with the given db backup file."""
        print(f"Restoring from {file}...")
        self.dbpath.write_bytes(Pathier(file).read_bytes())
        print("Restore complete.")

    @argshell.with_parser(dbparsers.get_scan_dbs_parser)
    def do_scan_dbs(self, args: argshell.Namespace):
        """Scan the current working directory for database files."""
        cwd = Pathier.cwd()
        dbs = []
        globber = cwd.glob
        if args.recursive:
            cwd.rglob
        for extension in args.extensions:
            dbs.extend(list(globber(f"*{extension}")))
        for db in dbs:
            print(db.separate(cwd.stem))

    @argshell.with_parser(dbparsers.get_select_parser, [dbparsers.select_post_parser])
    def do_select(self, args: argshell.Namespace):
        """Execute a SELECT query with the given args."""
        print(f"Searching {args.table}... ")
        with Databased(self.dbpath) as db:
            rows = db.select(
                args.table,
                args.columns,
                args.joins,
                args.where,
                args.group_by,
                args.having,
                args.order_by,
                limit=args.limit,
            )
            print(f"Found {len(rows)} rows:")
            self.display(rows)
            print(f"{len(rows)} rows from {args.table}")

    def do_size(self, _: str):
        """Display the size of the the current db file."""
        print(f"{self.dbpath.name} is {self.dbpath.formatted_size}.")

    @argshell.with_parser(dbparsers.get_update_parser)
    def do_update(self, args: argshell.Namespace):
        """Update a column to a new value.

        Syntax:
        >>> update {table} {column} {value} {where}
        >>> based>update users username big_chungus "username = lil_chungus"

        ^will update the username in the users 'table' to 'big_chungus' where the username is currently 'lil_chungus'^"""
        print("Updating rows...")
        with Databased(self.dbpath) as db:
            num_updates = db.update(args.table, args.column, args.new_value, args.where)
            print(f"Updated {num_updates} rows in table {args.table}.")

    def do_use(self, arg: str):
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

    def do_vacuum(self, _: str):
        """Reduce database disk memory."""
        print(f"Database size before vacuuming: {self.dbpath.formatted_size}")
        print("Vacuuming database...")
        with Databased(self.dbpath) as db:
            freedspace = db.vacuum()
        print(f"Database size after vacuuming: {self.dbpath.formatted_size}")
        print(f"Freed up {Pathier.format_bytes(freedspace)} of disk space.")

    # Seat

    def _choose_db(self, options: list[Pathier]) -> Pathier:
        """Prompt the user to select from a list of files."""
        cwd = Pathier.cwd()
        paths = [path.separate(cwd.stem) for path in options]
        while True:
            print(
                f"DB options:\n{' '.join([f'({i}) {path}' for i, path in enumerate(paths, 1)])}"
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
