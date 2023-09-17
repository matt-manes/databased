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


def get_select_parser(args: argshell.Namespace) -> argshell.ArgShellParser:
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
