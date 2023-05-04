import logging
import os
import sqlite3
from datetime import datetime
from functools import wraps
from typing import Any

import pandas
from pathier import Pathier
from tabulate import tabulate


def _connect(func):
    """Decorator to open db connection if it isn't already open."""

    @wraps(func)
    def inner(self, *args, **kwargs):
        if not self.connection_open:
            self.open()
        results = func(self, *args, **kwargs)
        return results

    return inner


class DataBased:
    """Sqli wrapper so queries don't need to be written except table definitions.

    Supports saving and reading dates as datetime objects.

    Supports using a context manager."""

    def __init__(
        self,
        dbpath: str | Pathier,
        logger_encoding: str = "utf-8",
        logger_message_format: str = "{levelname}|-|{asctime}|-|{message}",
    ):
        """
        #### :params:

        `dbpath`: String or Path object to database file.
        If a relative path is given, it will be relative to the
        current working directory. The log file will be saved to the
        same directory.

        `logger_message_format`: '{' style format string for the logger object."""
        self.dbpath = Pathier(dbpath)
        self.dbname = Pathier(dbpath).name
        self.dbpath.parent.mkdir(parents=True, exist_ok=True)
        self._logger_init(
            encoding=logger_encoding, message_format=logger_message_format
        )
        self.connection_open = False

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    def open(self):
        """Open connection to db."""
        self.connection = sqlite3.connect(
            self.dbpath,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            timeout=10,
        )
        self.connection.execute("pragma foreign_keys = 1;")
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

    def _logger_init(
        self,
        message_format: str = "{levelname}|-|{asctime}|-|{message}",
        encoding: str = "utf-8",
    ):
        """:param `message_format`: '{' style format string"""
        self.logger = logging.getLogger(self.dbname)
        if not self.logger.hasHandlers():
            handler = logging.FileHandler(
                str(self.dbpath).replace(".", "") + ".log", encoding=encoding
            )
            handler.setFormatter(
                logging.Formatter(
                    message_format, style="{", datefmt="%m/%d/%Y %I:%M:%S %p"
                )
            )
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _get_dict(
        self, table: str, values: list, columns_to_return: list[str] | None = None
    ) -> dict:
        """Converts the values of a row into a dictionary with column names as keys.

        #### :params:

        `table`: The table that values were pulled from.

        `values`: List of values expected to be the same quantity
        and in the same order as the column names of table.

        `columns_to_return`: An optional list of column names.
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

        #### :params:

        `match_criteria`: Can be a list of 2-tuples where each
        tuple is `(columnName, rowValue)` or a dictionary where
        keys are column names and values are row values.

        `exact_match`: If `False`, the row value for a given column
        will be matched as a substring.

        Usage e.g.:

        >>> self.cursor.execute(f'select * from {table} where {conditions};')"""
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
    def query(self, query_) -> list[Any]:
        """Execute an arbitrary query and return the results."""
        self.cursor.execute(query_)
        return self.cursor.fetchall()

    @_connect
    def create_tables(self, table_defs: list[str] = []):
        """Create tables if they don't exist.

        :param `table_defs`: Each definition should be in the form `table_name(column_definitions)`"""
        if len(table_defs) > 0:
            table_names = self.get_table_names()
            for table in table_defs:
                if table.split("(")[0].strip() not in table_names:
                    self.cursor.execute(f"create table {table};")
                    self.logger.info(f'{table.split("(")[0]} table created.')

    @_connect
    def create_table(self, table: str, column_defs: list[str]):
        """Create a table if it doesn't exist.

        #### :params:

        `table`: Name of the table to create.

        `column_defs`: List of column definitions in proper Sqlite3 sytax.
        i.e. `"column_name text unique"` or `"column_name int primary key"` etc."""
        if table not in self.get_table_names():
            query = f"create table {table}({', '.join(column_defs)});"
            self.cursor.execute(query)
            self.logger.info(f"'{table}' table created.")

    @_connect
    def get_table_names(self) -> list[str]:
        """Returns a list of table names from the database."""
        self.cursor.execute(
            'select name from sqlite_Schema where type = "table" and name not like "sqlite_%";'
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
        match_criteria: list[tuple] | dict | None = None,
        exact_match: bool = True,
    ) -> int:
        """Return number of items in `table`.

        #### :params:

        `match_criteria`: Can be a list of 2-tuples where each
        tuple is `(columnName, rowValue)` or a dictionary where
        keys are column names and values are row values.
        If `None`, all rows from the table will be counted.

        `exact_match`: If `False`, the row value for a given column
        in `match_criteria` will be matched as a substring.
        Has no effect if `match_criteria` is `None`.
        """
        query = f"select count(_rowid_) from {table}"
        try:
            if match_criteria:
                self.cursor.execute(
                    f"{query} where {self._get_conditions(match_criteria, exact_match)};"
                )
            else:
                self.cursor.execute(f"{query}")
            return self.cursor.fetchone()[0]
        except:
            return 0

    @_connect
    def add_row(
        self, table: str, values: tuple[Any], columns: tuple[str] | None = None
    ):
        """Add a row of values to a table.

        #### :params:

        `table`: The table to insert values into.

        `values`: A tuple of values to be inserted into the table.

        `columns`: If `None`, `values` is expected to supply a value for every column in the table.
        If `columns` is provided, it should contain the same number of elements as `values`."""
        parameterizer = ", ".join("?" for _ in values)
        logger_values = ", ".join(str(value) for value in values)
        try:
            if columns:
                columns_query = ", ".join(column for column in columns)
                self.cursor.execute(
                    f"insert into {table} ({columns_query}) values({parameterizer});",
                    values,
                )
            else:
                self.cursor.execute(
                    f"insert into {table} values({parameterizer});", values
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
        match_criteria: list[tuple] | dict | None = None,
        exact_match: bool = True,
        sort_by_column: str | None = None,
        columns_to_return: list[str] | None = None,
        return_as_dataframe: bool = False,
        values_only: bool = False,
        order_by: str | None = None,
        limit: str | int | None = None,
    ) -> list[dict] | list[tuple] | pandas.DataFrame:
        """Return matching rows from `table`.

        By default, rows will be returned as a list of dictionaries of the form `[{"column_name": value, ...}, ...]`


        #### :params:

        `match_criteria`: Can be a list of 2-tuples where each
        tuple is `(columnName, rowValue)` or a dictionary where
        keys are column names and values are row values.

        `exact_match`: If `False`, the row value for a given column will be matched as a substring.

        `sort_by_column`: A column name to sort the results by.
        This will sort results in Python after retrieving them from the db.
        Use the 'order_by' param to use SQLite engine for ordering.

        `columns_to_return`: Optional list of column names.
        If provided, the elements returned by this function will only contain the provided columns.
        Otherwise every column in the row is returned.

        `return_as_dataframe`: Return the results as a `pandas.DataFrame` object.

        `values_only`: Return the results as a list of tuples.

        `order_by`: If given, a `order by {order_by}` clause will be added to the select query.

        `limit`: If given, a `limit {limit}` clause will be added to the select query.
        """

        if type(columns_to_return) is str:
            columns_to_return = [columns_to_return]
        query = f"select * from {table}"
        matches = []
        if match_criteria:
            query += f" where {self._get_conditions(match_criteria, exact_match)}"
        if order_by:
            query += f" order by {order_by}"
        if limit:
            query += f" limit {limit}"
        query += ";"
        self.cursor.execute(query)
        matches = self.cursor.fetchall()
        results = [self._get_dict(table, match, columns_to_return) for match in matches]
        if sort_by_column:
            results = sorted(results, key=lambda x: x[sort_by_column])
        if return_as_dataframe:
            return pandas.DataFrame(results)
        if values_only:
            return [tuple(row.values()) for row in results]
        else:
            return results

    @_connect
    def find(
        self, table: str, query_string: str, columns: list[str] | None = None
    ) -> list[dict]:
        """Search for rows that contain `query_string` as a substring of any column.

        #### :params:

        `table`: The table to search.

        `query_string`: The substring to search for in all columns.

        `columns`: A list of columns to search for query_string.
        If None, all columns in the table will be searched.
        """
        if type(columns) is str:
            columns = [columns]
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
        return results

    @_connect
    def delete(
        self, table: str, match_criteria: list[tuple] | dict, exact_match: bool = True
    ) -> int:
        """Delete records from `table`.

        Returns the number of deleted records.

        #### :params:

        `match_criteria`: Can be a list of 2-tuples where each tuple is `(column_name, value)`
        or a dictionary where keys are column names and values are corresponding values.

        `exact_match`: If `False`, the value for a given column will be matched as a substring.
        """
        conditions = self._get_conditions(match_criteria, exact_match)
        try:
            self.cursor.execute(f"delete from {table} where {conditions};")
            num_deletions = self.cursor.rowcount
            self.logger.info(
                f'Deleted {num_deletions} rows from "{table}" where {conditions}".'
            )
            return num_deletions
        except Exception as e:
            self.logger.debug(
                f'Error deleting rows from "{table}" where {conditions}.\n{e}'
            )
            return 0

    @_connect
    def update(
        self,
        table: str,
        column_to_update: str,
        new_value: Any,
        match_criteria: list[tuple] | dict | None = None,
    ) -> bool:
        """Update the value in `column_to_update` to `new_value` for rows matched with `match_criteria`.

        #### :params:

        `table`: The table to update rows in.

        `column_to_update`: The column to be updated in the matched rows.

        `new_value`: The new value to insert.

        `match_criteria`: Can be a list of 2-tuples where each tuple is `(columnName, rowValue)`
        or a dictionary where keys are column names and values are corresponding values.
        If `None`, every row in `table` will be updated.

        Returns `True` if successful, `False` if not."""
        query = f"update {table} set {column_to_update} = ?"
        conditions = ""
        if match_criteria:
            if self.count(table, match_criteria) == 0:
                self.logger.info(
                    f"Couldn't find matching records in {table} table to update to '{new_value}'"
                )
                return False
            conditions = self._get_conditions(match_criteria)
            query += f" where {conditions}"
        else:
            conditions = None
        query += ";"
        try:
            self.cursor.execute(
                query,
                (new_value,),
            )
            self.logger.info(
                f'Updated "{column_to_update}" in "{table}" table to "{new_value}" where {conditions}'
            )
            return True
        except Exception as e:
            self.logger.error(
                f'Failed to update "{column_to_update}" in "{table}" table to "{new_value}" where {conditions}"\n{e}'
            )
            return False

    @_connect
    def drop_table(self, table: str) -> bool:
        """Drop `table` from the database.

        Returns `True` if successful, `False` if not."""
        try:
            self.cursor.execute(f"drop Table {table};")
            self.logger.info(f'Dropped table "{table}"')
            return True
        except Exception as e:
            print(e)
            self.logger.error(f'Failed to drop table "{table}"')
            return False

    @_connect
    def add_column(
        self, table: str, column: str, _type: str, default_value: str | None = None
    ):
        """Add a new column to `table`.

        #### :params:

        `column`: Name of the column to add.

        `_type`: The data type of the new column.

        `default_value`: Optional default value for the column."""
        try:
            if default_value:
                self.cursor.execute(
                    f"alter table {table} add column {column} {_type} default {default_value};"
                )
                self.update(table, column, default_value)
            else:
                self.cursor.execute(f"alter table {table} add column {column} {_type};")
            self.logger.info(f'Added column "{column}" to "{table}" table.')
        except Exception as e:
            self.logger.error(f'Failed to add column "{column}" to "{table}" table.')

    @staticmethod
    def data_to_string(
        data: list[dict], sort_key: str | None = None, wrap_to_terminal: bool = True
    ) -> str:
        """Uses tabulate to produce pretty string output from a list of dictionaries.

        #### :params:

        `data`: The list of dictionaries to create a grid from.
        Assumes all dictionaries in list have the same set of keys.

        `sort_key`: Optional dictionary key to sort data with.

        `wrap_to_terminal`: If `True`, the table width will be wrapped to fit within the current terminal window.
        Pass as `False` if the output is going into something like a `.txt` file."""
        return data_to_string(data, sort_key, wrap_to_terminal)


def data_to_string(
    data: list[dict], sort_key: str | None = None, wrap_to_terminal: bool = True
) -> str:
    """Uses tabulate to produce pretty string output from a list of dictionaries.

    #### :params:

    `data`: The list of dictionaries to create a grid from.
    Assumes all dictionaries in list have the same set of keys.

    `sort_key`: Optional dictionary key to sort data with.

    `wrap_to_terminal`: If `True`, the table width will be wrapped to fit within the current terminal window.
    Pass as `False` if the output is going into something like a `.txt` file."""
    if len(data) == 0:
        return ""
    if sort_key:
        data = sorted(data, key=lambda d: d[sort_key])
    for i, d in enumerate(data):
        for k in d:
            data[i][k] = str(data[i][k])

    too_wide = True
    terminal_width = os.get_terminal_size().columns
    max_col_widths = terminal_width
    # Make an output with effectively unrestricted column widths
    # to see if shrinking is necessary
    output = tabulate(
        data,
        headers="keys",
        disable_numparse=True,
        tablefmt="grid",
        maxcolwidths=max_col_widths,
    )
    current_width = output.index("\n")
    if current_width < terminal_width:
        too_wide = False
    if wrap_to_terminal and too_wide:
        print("Resizing grid to fit within the terminal...\n")
        previous_col_widths = max_col_widths
        acceptable_width = terminal_width - 10
        while too_wide and max_col_widths > 1:
            if current_width >= terminal_width:
                previous_col_widths = max_col_widths
                max_col_widths = int(max_col_widths * 0.5)
            elif current_width < terminal_width:
                # Without lowering acceptable_width, this condition will cause infinite loop
                if max_col_widths == previous_col_widths - 1:
                    acceptable_width -= 10
                max_col_widths = int(
                    max_col_widths + (0.5 * (previous_col_widths - max_col_widths))
                )
            output = tabulate(
                data,
                headers="keys",
                disable_numparse=True,
                tablefmt="grid",
                maxcolwidths=max_col_widths,
            )
            current_width = output.index("\n")
            if acceptable_width < current_width < terminal_width:
                too_wide = False
        if too_wide:
            print("Couldn't resize grid to fit within the terminal.")
            return str(data)
    return output
