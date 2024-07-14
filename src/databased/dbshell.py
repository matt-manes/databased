from datetime import datetime

import argshell
from noiftimer import time_it
from pathier import Pathier, Pathish
from printbuddies import Grid
from rich.console import Console

import databased.dbparsers as dbparsers

from .create_shell import create_shell
from .databased import Databased, Rows

console = Console()


class DBShell(argshell.ArgShell):
    _dbpath: Pathier = None  # type: ignore
    connection_timeout: float = 10
    detect_types: bool = True
    enforce_foreign_keys: bool = True
    commit_on_close: bool = True
    log_dir: Pathier = Pathier.cwd()
    intro = f"Starting dbshell (enter help or ? for arg info)...\n"
    prompt = f"based>"

    @property
    def dbpath(self) -> Pathier:
        return self._dbpath

    @dbpath.setter
    def dbpath(self, path: Pathish):
        self._dbpath = Pathier(path)
        self.prompt = f"{self._dbpath.name}>"

    def _DB(self) -> Databased:
        return Databased(
            self.dbpath,
            self.connection_timeout,
            self.detect_types,
            self.enforce_foreign_keys,
            self.commit_on_close,
            self.log_dir,
        )

    @time_it()
    def default(self, line: str):
        line = line.strip("_")
        with self._DB() as db:
            self.display(db.query(line))

    def display(self, data: Rows):
        """Print row data to terminal in a grid."""
        if not data or len(data) == 0:
            self.console.print("Nothing to display.")
            return
        try:
            self.console.print(Grid(data, cast_values_to_strings=True))
        except Exception as e:
            self.console.print("Could not fit data into grid :(")
            self.console.print(e)

    # Seat

    def _show_tables(self, args: argshell.Namespace):
        with self._DB() as db:
            if args.tables:
                tables = [table for table in args.tables if table in db.tables]
            else:
                tables = db.tables
            if tables:
                self.console.print("Getting database tables...")
                info = [
                    {
                        "Table Name": table,
                        "Columns": ", ".join(db.get_columns(table)),
                        "Number of Rows": db.count(table) if args.rowcount else "n/a",
                    }
                    for table in tables
                ]
                self.display(info)

    def _show_views(self, args: argshell.Namespace):
        with self._DB() as db:
            if args.tables:
                views = [view for view in args.tables if view in db.views]
            else:
                views = db.views
            if views:
                self.console.print("Getting database views...")
                info = [
                    {
                        "View Name": view,
                        "Columns": ", ".join(db.get_columns(view)),
                        "Number of Rows": db.count(view) if args.rowcount else "n/a",
                    }
                    for view in views
                ]
                self.display(info)

    @argshell.with_parser(dbparsers.get_add_column_parser)
    def do_add_column(self, args: argshell.Namespace):
        """Add a new column to the specified tables."""
        with self._DB() as db:
            db.add_column(args.table, args.column_def)

    @argshell.with_parser(dbparsers.get_add_table_parser)
    def do_add_table(self, args: argshell.Namespace):
        """Add a new table to the database."""
        with self._DB() as db:
            db.create_table(args.table, *args.columns)

    @argshell.with_parser(dbparsers.get_backup_parser)
    @time_it()
    def do_backup(self, args: argshell.Namespace):
        """Create a backup of the current db file."""
        self.console.print(f"Creating a back up for {self.dbpath}...")
        backup_path = self.dbpath.backup(args.timestamp)
        self.console.print("Creating backup is complete.")
        self.console.print(f"Backup path: {backup_path}")

    @argshell.with_parser(dbparsers.get_count_parser)
    @time_it()
    def do_count(self, args: argshell.Namespace):
        """Count the number of matching records."""
        with self._DB() as db:
            count = db.count(args.table, args.column, args.where, args.distinct)
            self.display(
                [
                    {
                        "Table": args.table,
                        "Column": args.column,
                        "Distinct": args.distinct,
                        "Where": args.where,
                        "Count": count,
                    }
                ]
            )

    def do_customize(self, name: str):
        """Generate a template file in the current working directory for creating a custom DBShell class.
        Expects one argument: the name of the custom dbshell.
        This will be used to name the generated file as well as several components in the file content.
        """
        try:
            create_shell(name)
        except Exception as e:
            self.console.print(f"{type(e).__name__}: {e}")

    def do_dbpath(self, _: str):
        """Print the .db file in use."""
        self.console.print(self.dbpath)

    @argshell.with_parser(dbparsers.get_delete_parser)
    @time_it()
    def do_delete(self, args: argshell.Namespace):
        """Delete rows from the database.

        Syntax:
        >>> delete {table} {where}
        >>> based>delete users "username LIKE '%chungus%"

        ^will delete all rows in the 'users' table whose username contains 'chungus'^"""
        self.console.print("Deleting records...")
        with self._DB() as db:
            num_rows = db.delete(args.table, args.where)
            self.console.print(f"Deleted {num_rows} rows from {args.table} table.")

    def do_describe(self, tables: str):
        """Describe each given table or view. If no list is given, all tables and views will be described."""
        with self._DB() as db:
            table_list = tables.split() or (db.tables + db.views)
            for table in table_list:
                self.console.print(f"<{table}>")
                self.console.print(db.to_grid(db.describe(table)))

    @argshell.with_parser(dbparsers.get_drop_column_parser)
    def do_drop_column(self, args: argshell.Namespace):
        """Drop the specified column from the specified table."""
        with self._DB() as db:
            db.drop_column(args.table, args.column)

    def do_drop_table(self, table: str):
        """Drop the specified table."""
        with self._DB() as db:
            db.drop_table(table)

    @argshell.with_parser(dbparsers.get_dump_parser)
    @time_it()
    def do_dump(self, args: argshell.Namespace):
        """Create `.sql` dump files for the current database."""
        date = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
        if not args.data_only:
            self.console.print("Dumping schema...")
            with self._DB() as db:
                db.dump_schema(
                    Pathier.cwd() / f"{db.name}_schema_{date}.sql", args.tables
                )
        if not args.schema_only:
            self.console.print("Dumping data...")
            with self._DB() as db:
                db.dump_data(Pathier.cwd() / f"{db.name}_data_{date}.sql", args.tables)

    def do_flush_log(self, _: str):
        """Clear the log file for this database."""
        log_path = self.dbpath.with_name(self.dbpath.name.replace(".", "") + ".log")
        if not log_path.exists():
            self.console.print(f"No log file at path {log_path}")
        else:
            self.console.print(f"Flushing log...")
            log_path.write_text("")

    def do_help(self, arg: str):
        """Display help messages."""
        super().do_help(arg)
        if arg == "":
            self.console.print("Unrecognized commands will be executed as queries.")
            self.console.print(
                "Use the `query` command explicitly if you don't want to capitalize your key words."
            )
            self.console.print(
                "All transactions initiated by commands are committed immediately."
            )
        self.console.print()

    def do_new_db(self, dbname: str):
        """Create a new, empty database with the given name."""
        dbpath = Pathier(dbname)
        self.dbpath = dbpath
        self.prompt = f"{self.dbpath.name}>"

    def do_properties(self, _: str):
        """See current database property settings."""
        for property_ in [
            "connection_timeout",
            "detect_types",
            "enforce_foreign_keys",
            "commit_on_close",
            "log_dir",
        ]:
            self.console.print(f"{property_}: {getattr(self, property_)}")

    @time_it()
    def do_query(self, query: str):
        """Execute a query against the current database."""
        self.console.print(f"Executing {query}")
        with self._DB() as db:
            results = db.query(query)
        self.display(results)
        self.console.print(f"{db.cursor.rowcount} affected rows")

    @argshell.with_parser(dbparsers.get_rename_column_parser)
    def do_rename_column(self, args: argshell.Namespace):
        """Rename a column."""
        with self._DB() as db:
            db.rename_column(args.table, args.column, args.new_name)

    @argshell.with_parser(dbparsers.get_rename_table_parser)
    def do_rename_table(self, args: argshell.Namespace):
        """Rename a table."""
        with self._DB() as db:
            db.rename_table(args.table, args.new_name)

    def do_restore(self, file: str):
        """Replace the current db file with the given db backup file."""
        backup = Pathier(file.strip('"'))
        if not backup.exists():
            self.console.print(f"{backup} does not exist.")
        else:
            self.console.print(f"Restoring from {file}...")
            self.dbpath.write_bytes(backup.read_bytes())
            self.console.print("Restore complete.")

    @argshell.with_parser(dbparsers.get_scan_dbs_parser)
    def do_scan(self, args: argshell.Namespace):
        """Scan the current working directory for database files."""
        dbs = self._scan(args.extensions, args.recursive)
        for db in dbs:
            self.console.print(db.separate(Pathier.cwd().stem))

    @argshell.with_parser(dbparsers.get_schema_parser)
    @time_it()
    def do_schema(self, args: argshell.Namespace):
        """Print out the names of the database tables and views, their columns, and, optionally, the number of rows."""
        self._show_tables(args)
        self._show_views(args)

    @time_it()
    def do_script(self, path: str):
        """Execute the given SQL script."""
        with self._DB() as db:
            self.display(db.execute_script(path))

    @argshell.with_parser(dbparsers.get_select_parser, [dbparsers.select_post_parser])
    @time_it()
    def do_select(self, args: argshell.Namespace):
        """Execute a SELECT query with the given args."""
        self.console.print(f"Querying {args.table}... ")
        with self._DB() as db:
            rows = db.select(
                table=args.table,
                columns=args.columns,
                joins=args.joins,
                where=args.where,
                group_by=args.group_by,
                having=args.Having,
                order_by=args.order_by,
                limit=args.limit,
                exclude_columns=args.exclude_columns,
            )
            self.console.print(f"Found {len(rows)} rows:")
            self.display(rows)
            self.console.print(f"{len(rows)} rows from {args.table}")

    def do_set_connection_timeout(self, seconds: str):
        """Set database connection timeout to this number of seconds."""
        self.connection_timeout = float(seconds)

    def do_set_detect_types(self, should_detect: str):
        """Pass a `1` to turn on and a `0` to turn off."""
        self.detect_types = bool(int(should_detect))

    def do_set_enforce_foreign_keys(self, should_enforce: str):
        """Pass a `1` to turn on and a `0` to turn off."""
        self.enforce_foreign_keys = bool(int(should_enforce))

    def do_set_commit_on_close(self, should_commit: str):
        """Pass a `1` to turn on and a `0` to turn off."""
        self.commit_on_close = bool(int(should_commit))

    def do_size(self, _: str):
        """Display the size of the the current db file."""
        self.console.print(f"{self.dbpath.name} is {self.dbpath.formatted_size}.")

    @argshell.with_parser(dbparsers.get_schema_parser)
    @time_it()
    def do_tables(self, args: argshell.Namespace):
        """Print out the names of the database tables, their columns, and, optionally, the number of rows."""
        self._show_tables(args)

    @argshell.with_parser(dbparsers.get_update_parser)
    @time_it()
    def do_update(self, args: argshell.Namespace):
        """Update a column to a new value.

        Syntax:
        >>> update {table} {column} {value} {where}
        >>> based>update users username big_chungus "username = lil_chungus"

        ^will update the username in the users 'table' to 'big_chungus' where the username is currently 'lil_chungus'^
        """
        self.console.print("Updating rows...")
        with self._DB() as db:
            num_updates = db.update(args.table, args.column, args.new_value, args.where)
            self.console.print(f"Updated {num_updates} rows in table {args.table}.")

    def do_use(self, dbname: str):
        """Set which database file to use."""
        dbpath = Pathier(dbname)
        if not dbpath.exists():
            self.console.print(f"{dbpath} does not exist.")
            self.console.print(f"Still using {self.dbpath}")
        elif not dbpath.is_file():
            self.console.print(f"{dbpath} is not a file.")
            self.console.print(f"Still using {self.dbpath}")
        else:
            self.dbpath = dbpath
            self.prompt = f"{self.dbpath.name}>"

    @time_it()
    def do_vacuum(self, _: str):
        """Reduce database disk memory."""
        self.console.print(
            f"Database size before vacuuming: {self.dbpath.formatted_size}"
        )
        self.console.print("Vacuuming database...")
        with self._DB() as db:
            freedspace = db.vacuum()
        self.console.print(
            f"Database size after vacuuming: {self.dbpath.formatted_size}"
        )
        self.console.print(
            f"Freed up {Pathier.format_bytes(freedspace)} of disk space."
        )

    @argshell.with_parser(dbparsers.get_schema_parser)
    @time_it()
    def do_views(self, args: argshell.Namespace):
        """Print out the names of the database views, their columns, and, optionally, the number of rows."""
        self._show_views(args)

    # Seat

    def _choose_db(self, options: list[Pathier]) -> Pathier:
        """Prompt the user to select from a list of files."""
        cwd = Pathier.cwd()
        paths = [path.separate(cwd.stem) for path in options]
        while True:
            self.console.print(
                f"DB options:\n{' '.join([f'({i}) {path}' for i, path in enumerate(paths, 1)])}"
            )
            choice = input("Enter the number of the option to use: ")
            try:
                index = int(choice)
                if not 1 <= index <= len(options):
                    self.console.print("Choice out of range.")
                    continue
                return options[index - 1]
            except Exception as e:
                self.console.print(f"{choice} is not a valid option.")

    def _scan(
        self, extensions: list[str] = [".sqlite3", ".db"], recursive: bool = False
    ) -> list[Pathier]:
        cwd = Pathier.cwd()
        dbs: list[Pathier] = []
        globber = cwd.glob
        if recursive:
            globber = cwd.rglob
        for extension in extensions:
            dbs.extend(list(globber(f"*{extension}")))
        return dbs

    def preloop(self):
        """Scan the current directory for a .db file to use.
        If not found, prompt the user for one or to try again recursively."""
        if self.dbpath:
            self.dbpath = Pathier(self.dbpath)
            self.console.print(f"Defaulting to database {self.dbpath}")
        else:
            self.console.print("Searching for database...")
            cwd = Pathier.cwd()
            dbs = self._scan()
            if len(dbs) == 1:
                self.dbpath = dbs[0]
                self.console.print(f"Using database {self.dbpath}.")
            elif dbs:
                self.dbpath = self._choose_db(dbs)
            else:
                self.console.print(f"Could not find a database file in {cwd}.")
                path = input(
                    "Enter path to database file to use (creating it if necessary) or press enter to search again recursively: "
                )
                if path:
                    self.dbpath = Pathier(path)
                elif not path:
                    self.console.print("Searching recursively...")
                    dbs = self._scan(recursive=True)
                    if len(dbs) == 1:
                        self.dbpath = dbs[0]
                        self.console.print(f"Using database {self.dbpath}.")
                    elif dbs:
                        self.dbpath = self._choose_db(dbs)
                    else:
                        self.console.print("Could not find a database file.")
                        self.dbpath = Pathier(
                            input(
                                "Enter path to a database file (creating it if necessary): "
                            )
                        )


def get_args() -> argshell.Namespace:
    parser = argshell.ArgumentParser()

    parser.add_argument(
        "dbpath",
        nargs="?",
        type=str,
        help=""" The database file to use. If not provided the current working directory will be scanned for database files. """,
    )
    args = parser.parse_args()

    return args


def main(args: argshell.Namespace | None = None):
    if not args:
        args = get_args()
    dbshell = DBShell()
    if args.dbpath:
        dbshell.dbpath = Pathier(args.dbpath)
    dbshell.cmdloop()


if __name__ == "__main__":
    main(get_args())
