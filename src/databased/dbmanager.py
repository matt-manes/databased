import argparse
import shlex
import sys
from pathlib import Path

from databased import DataBased, data_to_string

""" A command line tool to interact with a database file.
Works like a standard argparse based cli
except it will ask you for arguments indefinitely in a loop
instead of having to invoke the script with arguments over and over.
I.e. instead of "python dbManager.py -db database.db -f someString -t someTable",
just invoke "python dbManager.py" then a repeating "Enter command: " prompt will appear
and "-db database.db -f someString -t someTable" can be entered.
Note: a -db arg only needs to be provided once (if there is no default set) unless
you wish to change databases. So a subsequent command to the above
can just be entered as "-f someOtherString -t someTable".

This is just a quick template and can be customized by adding arguments and adding/overriding functions
for specific database projects."""

# Subclassing to prevent program exit when the -h/--help arg is passed.
class ArgParser(argparse.ArgumentParser):
    def exit(self, status=0, message=None):
        if message:
            self._print_message(message, sys.stderr)


def get_args(command: str) -> argparse.Namespace:
    parser = ArgParser()

    parser.add_argument(
        "-db",
        "--dbname",
        type=str,
        default=None,
        help="""Name of database file to use.
        Required on the first loop if no default is set,
        but subsequent loops will resuse the same database
        unless a new one is provided through this arg.""",
    )

    parser.add_argument(
        "-i",
        "--info",
        action="store_true",
        help=""" Display table names, their respective columns, and how many records they contain.
        If a -t/--tables arg is passed, just the columns and row count for those tables will be shown.""",
    )

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
        "-f",
        "--find",
        type=str,
        default=None,
        help=""" A substring to search the database for. 
        If a -c/--columns arg(s) is not given, the values will be matched against all columns.
        Similarly, if a -t/--tables arg(s) is not given, the values will be searched for in all tables.""",
    )

    parser.add_argument(
        "-sco",
        "--show_count_only",
        action="store_true",
        help=""" Show the number of results returned by -f/--find,
        but don't print the results to the terminal.""",
    )

    parser.add_argument(
        "-d",
        "--delete",
        type=str,
        nargs="*",
        default=[],
        help=""" A list of values to be deleted from the database.
        A -c/--columns arg must be supplied.
        A -t/--tables arg must be supplied.""",
    )

    parser.add_argument(
        "-u",
        "--update",
        type=str,
        default=None,
        nargs="*",
        help=""" Update a record in the database.
        Expects the first argument to be the new value and interprets
        subsequent arguements as pairs of 'column' and 'value' to use
        when selecting which rows to update. The -c/--columns arg will
        be the column that is updated with the new value for matching rows.
        A -c/--columns arg must be supplied.
        A -t/--tables arg must be supplied.
        e.g '-t birds -c last_seen -u today name sparrow migratory 0'
        will update the 'last_seen' column of the 'birds' table to 'today'
        for all rows that have either/both of their 'name' and 'migratory'
        columns set to 'sparrow' and '0', respectively.""",
    )

    parser.add_argument(
        "-sb", "--sort_by", type=str, default=None, help="Column to sort results by."
    )

    args = parser.parse_args(command)

    if args.dbname and not Path(args.dbname).exists():
        raise Exception(f"{args.dbname} does not exist.")

    return args


def info():
    print("Getting database info...")
    print()
    if not args.tables:
        args.tables = db.get_table_names()
    results = []
    for table in args.tables:
        count = db.count(table)
        columns = db.get_column_names(table)
        results.append(
            {
                "table name": table,
                "columns": ", ".join(columns),
                "number of rows": count,
            }
        )
    if args.sort_by and args.sort_by in results[0]:
        results = sorted(results, key=lambda x: x[args.sort_by])
    print(data_to_string(results))


def find():
    print("Finding records... ")
    print()
    if not args.tables:
        args.tables = db.get_table_names()
    for table in args.tables:
        results = db.find(table, args.find, args.columns)
        if args.sort_by and args.sort_by in results[0]:
            results = sorted(results, key=lambda x: x[args.sort_by])
        if args.columns:
            print(
                f"{len(results)} results for '{args.find}' in '{', '.join(args.columns)}' column(s) of '{table}' table:"
            )
        else:
            print(f"{len(results)} results for '{args.find}' in '{table}' table:")
        if not args.show_count_only:
            print(data_to_string(results))
        print()


def delete():
    if not args.tables:
        raise ValueError("Missing -t/--tables arg for -d/--delete function.")
    if not args.columns:
        raise ValueError("Missing -c/--columns arg for -d/--delete function.")
    print("Deleting records... ")
    print()
    num_deleted_records = 0
    failed_deletions = []
    for item in args.delete:
        success = db.delete(args.tables[0], [(args.columns[0], item)])
        if success:
            num_deleted_records += success
        else:
            failed_deletions.append(item)
    print(f"Deleted {num_deleted_records} record(s) from '{args.tables[0]}' table.")
    if len(failed_deletions) > 0:
        print(
            f"Failed to delete the following {len(failed_deletions)} record(s) from '{args.tables[0]}' table:"
        )
        for fail in failed_deletions:
            print(f"  {fail}")


def update():
    if not args.tables:
        raise ValueError("Missing -t/--tables arg for -u/--update function.")
    if not args.columns:
        raise ValueError("Missing -c/--columns arg for -u/--update function.")
    print("Updating record... ")
    print()
    new_value = args.update[0]
    if len(args.update) > 1:
        args.update = args.update[1:]
        match_criteria = [
            (args.update[i], args.update[i + 1]) for i in range(0, len(args.update), 2)
        ]
    else:
        match_criteria = None
    if db.update(
        args.tables[0],
        args.columns[0],
        new_value,
        match_criteria,
    ):
        print(
            f"Updated '{args.columns[0]}' column to '{new_value}' in '{args.tables[0]}' table for match_criteria {match_criteria}."
        )
    else:
        print(
            f"Failed to update '{args.columns[0]}' column to '{new_value}' in '{args.tables[0]}' table for match_criteria {match_criteria}."
        )


def print_table():
    for table in args.tables:
        rows = db.get_rows(
            table, columns_to_return=args.columns, sort_by_column=args.sort_by
        )
        print(f"{table} table:")
        print(data_to_string(rows))


if __name__ == "__main__":
    sys.tracebacklimit = 0
    while True:
        try:
            command = shlex.split(input("Enter command: "))
            args = get_args(command)
            if args.dbname:
                dbname = args.dbname
            with DataBased(dbpath=dbname) as db:
                if args.info:
                    info()
                elif args.find:
                    find()
                elif args.delete:
                    delete()
                elif args.update:
                    update()
                else:
                    print_table()
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(e)
