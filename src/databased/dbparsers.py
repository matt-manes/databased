import argshell

""" Parser building functions for DBShell """


def _get_base_parser(add_help: bool = False) -> argshell.ArgShellParser:
    """Returns base parser containing -t/--tables and -m/--match_pairs switches."""
    parser = argshell.ArgShellParser(add_help=add_help)
    parser.add_argument(
        "-t",
        "--tables",
        type=str,
        nargs="*",
        default=[],
        help="""Limits command to a specific list of tables""",
    )
    parser.add_argument(
        "-m",
        "--match_pairs",
        type=str,
        nargs="*",
        default=[],
        help=""" Pairs of columns and values to use for narrowing the scope of row operations.
        i.e. 'find -t users -m name Bob state Alaska last_login *' will print
        all rows from the users table that have the name Bob,
        are from the state Alaska, and last logged in at any date.""",
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


def get_create_table_parser(add_help: bool = True) -> argshell.ArgShellParser:
    """Returns a parser for adding a table to the database."""
    parser = argshell.ArgShellParser(add_help=add_help)
    parser.add_argument(
        "table_name", type=str, help=""" The name of the table to add. """
    )
    parser.add_argument(
        "-c",
        "--columns",
        type=str,
        nargs="*",
        default=[],
        help=""" A list of column definitions for the table.
        i.e. 'mytable -c "username text unique" "date_registered timestamp" "email_verified int default 0"'""",
    )
    return parser


def get_add_row_parser() -> argshell.ArgShellParser:
    """Returns a parser for adding a row to a table."""
    parser = argshell.ArgShellParser(parents=[get_create_table_parser(False)])
    parser.add_argument(
        "-v",
        "--values",
        nargs="*",
        default=[],
        help=""" If -c/--columns is supplied, it and this flag must have the same number of elements.
        If -c/--columns is not supplied, an element must be supplied to this flag for every column in the table. """,
    )
    return parser


def get_search_parser() -> argshell.ArgShellParser:
    """Returns a search parser."""
    parser = argshell.ArgShellParser()
    parser.add_argument(
        "search_string", type=str, help=""" Search all columns for this substring. """
    )
    parser.add_argument(
        "-t",
        "--tables",
        type=str,
        nargs="*",
        default=None,
        help="""Limits search to a specific list of tables""",
    )
    parser.add_argument(
        "-c",
        "--columns",
        type=str,
        nargs="*",
        default=None,
        help=""" Limits search to these columns. """,
    )
    return parser


def get_lookup_parser() -> argshell.ArgShellParser:
    """Returns a parser for row lookup functions."""
    parser = argshell.ArgShellParser(parents=[_get_base_parser()])
    parser.add_argument(
        "-c",
        "--columns",
        type=str,
        nargs="*",
        default=[],
        help=""" Limits what columns are returned.""",
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
    return parser


def get_update_parser() -> argshell.ArgShellParser:
    """Returns a parser for update function."""
    parser = argshell.ArgShellParser(parents=[_get_base_parser()])
    parser.add_argument(
        "-c", "--column", required=True, type=str, help=""" The column to update. """
    )
    parser.add_argument(
        "-v",
        "--new_value",
        required=True,
        help=""" The new value to update with. """,
    )
    return parser


def get_add_column_parser() -> argshell.ArgShellParser:
    """Returns a parser for the add column function."""
    parser = argshell.ArgShellParser()
    parser.add_argument(
        "column_name", type=str, help=""" The name of the column to add. """
    )
    parser.add_argument(
        "type", type=str, help=""" The data type of the column to add. """
    )
    parser.add_argument(
        "-t",
        "--tables",
        type=str,
        nargs="*",
        default=[],
        help="""Table(s) to add the column to.
        If not specified, the column will be added to every table in the database.""",
    )
    parser.add_argument(
        "-d",
        "--default_value",
        default=None,
        help=""" Default value for the new column. """,
    )
    return parser


# ============================================================post parsers============================================================
def convert_match_pairs(args: argshell.Namespace) -> argshell.Namespace:
    """Create a list of tuples from match_pairs."""
    if args.match_pairs:
        args.match_pairs = [
            (col, val)
            for col, val in zip(args.match_pairs[::2], args.match_pairs[1::2])
        ]
    return args


def verify_matching_length(args: argshell.Namespace) -> argshell.Namespace:
    """Raise an error if -c/--columns is greater than zero and has
    a different number of elements than -v/--values."""
    if args.columns and len(args.columns) != len(args.values):
        raise ValueError(
            f"Column/Value length mismatch: {len(args.columns)} columns and {len(args.values)} values provided."
        )
    return args
