import logging
import os
import sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any

from tabulate import tabulate


class DataBased:
    """Sqli wrapper so queries don't need to be written except table definitions.

    Supports saving and reading dates as datetime objects.

    Supports using a context manager."""

    def __init__(
        self,
        db_path: str | Path,
        logger_encoding: str = "utf-8",
        logger_message_format: str = "{levelname}|-|{asctime}|-|{message}",
    ):
        """
        :param db_path: String or Path object to database file.
        If a relative path is given, it will be relative to the
        current working directory. The log file will be saved to the
        same directory.

        :param logger_message_format: '{' style format string
        for the logger object."""
        self.db_path = Path(db_path)
        self.db_name = Path(db_path).name
        self._logger_init(
            encoding=logger_encoding, message_format=logger_message_format
        )
        self.connection_open = False
        self.create_manager()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    def create_manager(self):
        """Create dbManager.py in the same directory
        as the database file if they don't exist."""
        manager_template = Path(__file__).parent / "dbManager.py"
        manager_path = self.db_path.parent / "dbManager.py"
        if not manager_path.exists():
            manager_path.write_text(manager_template.read_text())

    def open(self):
        """Open connection to db."""
        self.connection = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            timeout=10,
        )
        self.connection.execute("pragma foreign_keys = 1")
        self.cursor = self.connection.cursor()
        self.connection_open = True

    def close(self):
        """Save and close connection to db.

        Call this as soon as you are done using the database if you have
        multiple threads or processes using the same database."""
        if self.connection_open:
            self.connection.commit()
            self.connection.close()
            self.connection_open = False

    def _connect(func):
        """Decorator to open db connection if it isn't already open."""

        @wraps(func)
        def inner(*args, **kwargs):
            self = args[0]
            if not self.connection_open:
                self.open()
            results = func(*args, **kwargs)
            return results

        return inner

    def _logger_init(
        self,
        message_format: str = "{levelname}|-|{asctime}|-|{message}",
        encoding: str = "utf-8",
    ):
        """:param message_format: '{' style format string"""
        self.logger = logging.getLogger(self.db_name)
        if not self.logger.hasHandlers():
            handler = logging.FileHandler(
                str(self.db_path).replace(".", "") + ".log", encoding=encoding
            )
            handler.setFormatter(
                logging.Formatter(
                    message_format, style="{", datefmt="%m/%d/%Y %I:%M:%S %p"
                )
            )
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _get_dict(
        self, table: str, values: list, columns_to_return: list[str] = None
    ) -> dict:
        """Converts the values of a row into a dictionary with column names as keys.

        :param table: The table that values were pulled from.

        :param values: List of values expected to be the same quantity
        and in the same order as the column names of table.

        :param columns_to_return: An optional list of column names.
        If given, only these columns will be included in the returned dictionary.
        Otherwise all columns and values are returned."""
        return {
            column: value
            for column, value in zip(self.get_column_names(table), values)
            if not columns_to_return or column in columns_to_return
        }

    def _get_conditions(
        self, match_criteria: list[tuple] | dict, exact_match: bool = True
    ) -> str:
        """Builds and returns the conditional portion of a query.

        :param match_criteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.

        :param exact_match: If False, the rowValue for a give column
        will be matched as a substring.

        Usage e.g.:

        self.cursor.execute(f'select * from {table} where {conditions}')"""
        if type(match_criteria) == dict:
            match_criteria = [(k, v) for k, v in match_criteria.items()]
        if exact_match:
            conditions = " and ".join(
                f'"{column_row[0]}" = "{column_row[1]}"'
                for column_row in match_criteria
            )
        else:
            conditions = " and ".join(
                f'"{column_row[0]}" like "%{column_row[1]}%"'
                for column_row in match_criteria
            )
        return f"({conditions})"

    @_connect
    def create_tables(self, table_statements: list[str] = []):
        """Create tables if they don't exist.

        :param table_statements: Each statement should be
        in the form 'tableName(columnDefinitions)'"""
        if len(table_statements) > 0:
            table_names = self.get_table_names()
            for table in table_statements:
                if table.split("(")[0].strip() not in table_names:
                    self.cursor.execute(f"create table {table}")
                    self.logger.info(f'{table.split("(")[0]} table created.')

    @_connect
    def create_table(self, table: str, column_defs: list[str]):
        """Create a table if it doesn't exist.

        :param table: Name of the table to create.

        :param column_defs: List of column definitions in
        proper Sqlite3 sytax.
        i.e. "columnName text unique" or "columnName int primary key" etc."""
        if table not in self.get_table_names():
            statement = f"{table}({', '.join(column_defs)})"
            self.cursor.execute(statement)
            self.logger.info(f"'{table}' table created.")

    @_connect
    def get_table_names(self) -> list[str]:
        """Returns a list of table names from database."""
        self.cursor.execute(
            'select name from sqlite_Schema where type = "table" and name not like "sqlite_%"'
        )
        return [result[0] for result in self.cursor.fetchall()]

    @_connect
    def get_column_names(self, table: str) -> list[str]:
        """Return a list of column names from a table."""
        self.cursor.execute(f"select * from {table} where 1=0")
        return [description[0] for description in self.cursor.description]

    @_connect
    def count(
        self,
        table: str,
        match_criteria: list[tuple] | dict = None,
        exact_match: bool = True,
    ) -> int:
        """Return number of items in table.

        :param match_criteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.
        If None, all rows from the table will be counted.

        :param exact_match: If False, the row value for a give column
        in match_criteria will be matched as a substring. Has no effect if
        match_criteria is None.
        """
        statement = f"select count(_rowid_) from {table}"
        try:
            if match_criteria:
                self.cursor.execute(
                    f"{statement} where {self._get_conditions(match_criteria, exact_match)}"
                )
            else:
                self.cursor.execute(f"{statement}")
            return self.cursor.fetchone()[0]
        except:
            return 0

    @_connect
    def add_row(self, table: str, values: tuple[any], columns: tuple[str] = None):
        """Add row of values to table.

        :param table: The table to insert into.

        :param values: A tuple of values to be inserted into the table.

        :param columns: If None, values param is expected to supply
        a value for every column in the table. If columns is
        provided, it should contain the same number of elements as values."""
        parameterizer = ", ".join("?" for _ in values)
        logger_values = ", ".join(str(value) for value in values)
        try:
            if columns:
                columns = ", ".join(column for column in columns)
                self.cursor.execute(
                    f"insert into {table} ({columns}) values({parameterizer})", values
                )
            else:
                self.cursor.execute(
                    f"insert into {table} values({parameterizer})", values
                )
            self.logger.info(f'Added "{logger_values}" to {table} table.')
        except Exception as e:
            if "constraint" not in str(e).lower():
                self.logger.exception(
                    f'Error adding "{logger_values}" to {table} table.'
                )
            else:
                self.logger.debug(str(e))

    @_connect
    def get_rows(
        self,
        table: str,
        match_criteria: list[tuple] | dict = None,
        exact_match: bool = True,
        sort_by_column: str = None,
        columns_to_return: list[str] = None,
        values_only: bool = False,
    ) -> tuple[dict] | tuple[tuple]:
        """Returns rows from table as a list of dictionaries
        where the key-value pairs of the dictionaries are
        column name: row value.

        :param match_criteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.

        :param exact_match: If False, the rowValue for a give column
        will be matched as a substring.

        :param sort_by_column: A column name to sort the results by.

        :param columns_to_return: Optional list of column names.
        If provided, the dictionaries returned by get_rows() will
        only contain the provided columns. Otherwise every column
        in the row is returned.

        :param values_only: Return the results as a tuple of tuples
        instead of a tuple of dictionaries that have column names as keys.
        The results will still be sorted according to sort_by_column if
        one is provided.
        """
        statement = f"select * from {table}"
        matches = []
        if not match_criteria:
            self.cursor.execute(statement)
        else:
            self.cursor.execute(
                f"{statement} where {self._get_conditions(match_criteria, exact_match)}"
            )
        matches = self.cursor.fetchall()
        results = tuple(
            self._get_dict(table, match, columns_to_return) for match in matches
        )
        if sort_by_column:
            results = tuple(sorted(results, key=lambda x: x[sort_by_column]))
        if values_only:
            return tuple(tuple(row.values()) for row in results)
        else:
            return results

    @_connect
    def find(
        self, table: str, query_string: str, columns: list[str] = None
    ) -> tuple[dict]:
        """Search for rows that contain query_string as a substring
        of any column.

        :param table: The table to search.

        :param query_string: The substring to search for in all columns.

        :param columns: A list of columns to search for query_string.
        If None, all columns in the table will be searched.
        """
        results = []
        if not columns:
            columns = self.get_column_names(table)
        for column in columns:
            results.extend(
                [
                    row
                    for row in self.get_rows(
                        table, [(column, query_string)], exact_match=False
                    )
                    if row not in results
                ]
            )
        return tuple(results)

    @_connect
    def delete(
        self, table: str, match_criteria: list[tuple] | dict, exact_match: bool = True
    ) -> int:
        """Delete records from table.

        Returns number of deleted records.

        :param match_criteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.

        :param exact_match: If False, the rowValue for a give column
        will be matched as a substring.
        """
        num_matches = self.count(table, match_criteria, exact_match)
        conditions = self._get_conditions(match_criteria, exact_match)
        try:
            self.cursor.execute(f"delete from {table} where {conditions}")
            self.logger.info(
                f'Deleted {num_matches} from "{table}" where {conditions}".'
            )
            return num_matches
        except Exception as e:
            self.logger.debug(f'Error deleting from "{table}" where {conditions}.\n{e}')
            return 0

    @_connect
    def update(
        self,
        table: str,
        column_to_update: str,
        new_value: Any,
        match_criteria: list[tuple] | dict = None,
    ) -> bool:
        """Update row value for entry matched with match_criteria.

        :param column_to_update: The column to be updated in the matched row.

        :param new_value: The new value to insert.

        :param match_criteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.
        If None, every row will be updated.

        Returns True if successful, False if not."""
        statement = f"update {table} set {column_to_update} = ?"
        if match_criteria:
            if self.count(table, match_criteria) == 0:
                self.logger.info(
                    f"Couldn't find matching records in {table} table to update to '{new_value}'"
                )
                return False
            conditions = self._get_conditions(match_criteria)
            statement += f" where {conditions}"
        else:
            conditions = None
        try:
            self.cursor.execute(
                statement,
                (new_value,),
            )
            self.logger.info(
                f'Updated "{column_to_update}" in "{table}" table to "{new_value}" where {conditions}'
            )
            return True
        except UnboundLocalError:
            table_filter_string = "\n".join(
                table_filter for table_filter in match_criteria
            )
            self.logger.error(
                f"No records found matching filters: {table_filter_string}"
            )
            return False
        except Exception as e:
            self.logger.error(
                f'Failed to update "{column_to_update}" in "{table}" table to "{new_value}" where {conditions}"\n{e}'
            )
            return False

    @_connect
    def drop_table(self, table: str) -> bool:
        """Drop a table from the database.

        Returns True if successful, False if not."""
        try:
            self.cursor.execute(f"drop Table {table}")
            self.logger.info(f'Dropped table "{table}"')
        except Exception as e:
            print(e)
            self.logger.error(f'Failed to drop table "{table}"')

    @_connect
    def add_column(
        self, table: str, column: str, _type: str, default_value: str = None
    ):
        """Add a new column to table.

        :param column: Name of the column to add.

        :param _type: The data type of the new column.

        :param default_value: Optional default value for the column."""
        try:
            if default_value:
                self.cursor.execute(
                    f"alter table {table} add column {column} {_type} default {default_value}"
                )
            else:
                self.cursor.execute(f"alter table {table} add column {column} {_type}")
            self.logger.info(f'Added column "{column}" to "{table}" table.')
        except Exception as e:
            self.logger.error(f'Failed to add column "{column}" to "{table}" table.')


def data_to_string(
    data: list[dict], sort_key: str = None, wrap_to_terminal: bool = True
) -> str:
    """Uses tabulate to produce pretty string output
    from a list of dictionaries.

    :param data: Assumes all dictionaries in list have the same set of keys.

    :param sort_key: Optional dictionary key to sort data with.

    :param wrap_to_terminal: If True, the table width will be wrapped
    to fit within the current terminal window. Set to False
    if the output is going into something like a txt file."""
    if len(data) == 0:
        return ""
    if sort_key:
        data = sorted(data, key=lambda d: d[sort_key])
    for i, d in enumerate(data):
        for k in d:
            data[i][k] = str(data[i][k])
    if wrap_to_terminal:
        terminal_width = os.get_terminal_size().columns
        max_col_widths = terminal_width
        """ Reducing the column width by tabulating one row at a time
        and then reducing further by tabulating the whole set proved to be 
        faster than going straight to tabulating the whole set and reducing
        the column width."""
        too_wide = True
        while too_wide and max_col_widths > 1:
            for i, row in enumerate(data):
                output = tabulate(
                    [row],
                    headers="keys",
                    disable_numparse=True,
                    tablefmt="grid",
                    maxcolwidths=max_col_widths,
                )
                if output.index("\n") > terminal_width:
                    max_col_widths -= 2
                    too_wide = True
                    break
                too_wide = False
    else:
        max_col_widths = None
    output = tabulate(
        data,
        headers="keys",
        disable_numparse=True,
        tablefmt="grid",
        maxcolwidths=max_col_widths,
    )
    # trim max column width until the output string is less wide than the current terminal width.
    if wrap_to_terminal:
        while output.index("\n") > terminal_width and max_col_widths > 1:
            max_col_widths -= 2
            max_col_widths = max(1, max_col_widths)
            output = tabulate(
                data,
                headers="keys",
                disable_numparse=True,
                tablefmt="grid",
                maxcolwidths=max_col_widths,
            )
    return output
