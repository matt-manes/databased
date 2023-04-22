import argshell
import dbparsers
from pathier import Pathier

import databased

root = Pathier(__file__).parent


class DBManager(argshell.ArgShell):
    intro = "Starting dbmanager (enter help or ? for command info)..."
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
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs
        Pass -h/--help flag for parser help."""
        print("Counting rows...")
        with databased.DataBased(self.dbname) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_rows = db.count(table, args.match_pairs, not args.partial_matching)
                print(f"{num_rows} matching rows in {table} table.")

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

    @argshell.with_parser(dbparsers.get_update_parser, [dbparsers.convert_match_pairs])
    def do_update(self, args: argshell.Namespace):
        """Update a column to a new value.
        Two required positional args: the column to update and the value to update to.
        Use the -t/--tables flag to limit what tables are updated.
        Use the -m/--match_pairs flag to specify which rows are updated.
        >>> based>update username big_chungus -t users -m username lil_chungus

        ^will update the username in table users to big_chungus where the username is currently lil_chungus^"""
        print("Updating rows...")
        with databased.DataBased(self.dbname) as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                if db.update(table, args.column, args.new_value, args.match_pairs):
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
