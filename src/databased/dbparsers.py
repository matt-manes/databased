import argshell

""" Parser building functions for DBShell """


def get_backup_parser() -> argshell.ArgShellParser:
    """Returns a parser for the backup command."""
    parser = argshell.ArgShellParser()
    parser.add_argument(
        "-t",
        "--timestamp",
        action="store_true",
        help=""" Add a timestamp to the backup file name to prevent overwriting previous backups. """,
    )
    return parser


def get_select_parser() -> argshell.ArgShellParser:
    """Returns a parser for use with `select`."""
    parser = argshell.ArgShellParser()
    parser.add_argument("table", type=str, help=""" The table to select from. """)
    parser.add_argument(
        "-c",
        "--columns",
        type=str,
        default=["*"],
        nargs="*",
        help=""" The columns to select. Should be given as a comma delimited string. If not given, `*` will be used. """,
    )
    parser.add_argument(
        "-j",
        "--joins",
        type=str,
        default=None,
        help=""" Joins to perform, if any. Should be in the form: `"{join type} JOIN {table2} ON {table}.{column} = {table2}.{column}"` """,
    )
    parser.add_argument(
        "-w",
        "--where",
        type=str,
        default=None,
        help=""" The `WHERE` clause to use, if any. Don't include "WHERE" keyword in argument string. """,
    )
    parser.add_argument(
        "-g",
        "--groub_by",
        type=str,
        default=None,
        nargs="*",
        help=""" The `GROUP BY` clause to use, if any. Don't include the keyword. """,
    )
    parser.add_argument(
        "-ha",
        "--having",
        type=str,
        default=None,
        help=""" The `HAVING` clause to use, if any. Don't include keyword. """,
    )
    parser.add_argument(
        "-o",
        "--order_by",
        type=str,
        default=None,
        nargs="*",
        help=""" The `ORDER BY` clause to use, if any. Don't include keyword. """,
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=str,
        default=None,
        help=""" The `LIMIT` clause to use, if any. Don't include keyword. """,
    )
    return parser


def select_post_parser(args: argshell.Namespace) -> argshell.Namespace:
    for field in ["columns", "group_by", "order_by"]:
        arglist = getattr(args, field)
        if arglist:
            setattr(args, field, ", ".join(arglist))
    return args


def get_drop_column_parser() -> argshell.ArgShellParser:
    parser = argshell.ArgShellParser()
    parser.add_argument(
        "table", type=str, help=""" The table to drop the column from. """
    )
    parser.add_argument("column", type=str, help=""" The column to drop. """)
    return parser


def get_info_parser() -> argshell.ArgShellParser:
    """Returns info parser."""
    parser = argshell.ArgShellParser()
    parser.add_argument(
        "-t",
        "--tables",
        type=str,
        nargs="*",
        default=[],
        help=""" Only display info for this table(s). """,
    )
    parser.add_argument(
        "-c",
        "--rowcount",
        action="store_true",
        help=""" Count and display the number of rows for each table. """,
    )
    return parser


def add_where_argument(parser: argshell.ArgShellParser) -> argshell.ArgShellParser:
    """Add an optional `where` argument to the parser and return it.

    The added argument has a default value of `None` and has `nargs="?"`"""
    parser.add_argument(
        "where",
        type=str,
        default=None,
        nargs="?",
        help=""" The `WHERE` clause to use, if any. Don't include "WHERE" keyword in argument string. """,
    )
    return parser


def get_update_parser() -> argshell.ArgShellParser:
    """Returns update parser."""
    parser = argshell.ArgShellParser()
    parser.add_argument("table", type=str, help=""" The table to update. """)
    parser.add_argument("column", type=str, help=""" The column to update. """)
    parser.add_argument(
        "value", type=str, help=""" The value to update the column to. """
    )
    parser = add_where_argument(parser)
    return parser


def get_delete_parser() -> argshell.ArgShellParser:
    """Returns delete parser."""
    parser = argshell.ArgShellParser()
    parser.add_argument("table", type=str, help=""" The table to delete from. """)
    parser = add_where_argument(parser)
    return parser


def get_add_column_parser() -> argshell.ArgShellParser:
    """Returns add column parser."""
    parser = argshell.ArgShellParser()
    parser.add_argument("table", type=str, help=""" The table to add a column to. """)
    parser.add_argument(
        "column_def",
        type=str,
        help=""" The column definition: {name} {type} {constraints} """,
    )
    return parser


def get_scan_dbs_parser() -> argshell.ArgShellParser:
    """Returns db scan parser."""
    parser = argshell.ArgShellParser()
    parser.add_argument(
        "-e",
        "--extensions",
        type=str,
        nargs="*",
        default=[".db", ".sqlite3"],
        help=""" A list of file extensions to scan for. By default, will scan for ".db" and ".sqlite3". """,
    )
    parser.add_argument(
        "-r", "--recursive", action="store_true", help=""" Scan recursively. """
    )
    return parser
