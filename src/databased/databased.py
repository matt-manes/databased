import sqlite3
from typing import Any, Iterable, Sequence

import loggi
from griddle import griddy
from pathier import Pathier, Pathish


def dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    fields = [column[0] for column in cursor.description]
    return {column: value for column, value in zip(fields, row)}


class Databased:
    """SQLite3 wrapper.

    Anytime `Databased.query()` is called, a connection to the database will be opened if it isn't already open.

    (All builtin class functions that access the database do so through the query method.)

    Connections, however, need to be closed manually.

    Manually closing the connection can be avoiding by using `Databased` with a context manager, which will close the connection upon exiting:
    >>> with Databased() as db:
    >>>     # connection closed
    >>>     rows = db.select("some_table")
    >>>     # connection opened
    >>> # connection closed

    Data is returned as a list of dictionaries where each dictionary is `{"column": value}`.

    """

    def __init__(
        self,
        dbpath: Pathish = "db.sqlite3",
        connection_timeout: float = 10,
        detect_types: bool = True,
        enforce_foreign_keys: bool = True,
        commit_on_close: bool = True,
        log_dir: Pathish | None = None,
    ):
        """
        :params:
        * `dbpath`: The path to the database file. Will be created if it doesn't exist.
        * `connection_timeout`: How long (in seconds) to wait before raising an exception when trying to connect to the database.
        * `detect_types`: Whether columns with values that can be converted to Python objects should be,
        i.e. `TIMESTAMP` table data can be recieved and is converted to, upon retrieval, a `datetime.datetime` object.
        * `enforce_foreign_keys`: Whether to enfore foreign key constraints.
        * `commit_on_close`: Whether to automatically commit transactions when the connection is closed.
        * `log_dir`: The directory the transaction log should be saved in. If `None`, it'll be saved in the same directory as the database file.
        """
        self.path = dbpath
        self.connection_timeout = connection_timeout
        self.connection = None
        self._logger_init(log_dir)
        self.detect_types = detect_types
        self.commit_on_close = commit_on_close
        self.enforce_foreign_keys = enforce_foreign_keys

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    @property
    def commit_on_close(self) -> bool:
        """Should commit database before closing connection when `self.close()` is called."""
        return self._commit_on_close

    @commit_on_close.setter
    def commit_on_close(self, should_commit_on_close: bool):
        self._commit_on_close = should_commit_on_close

    @property
    def connected(self) -> bool:
        """Whether this `Databased` instance is connected to the database file or not."""
        return self.connection is not None

    @property
    def connection_timeout(self) -> float:
        """Changes to this property won't take effect until the current connection, if open, is closed and a new connection opened."""
        return self._connection_timeout

    @connection_timeout.setter
    def connection_timeout(self, timeout: float):
        self._connection_timeout = timeout

    @property
    def detect_types(self) -> bool:
        """Should use `detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES` when establishing a database connection.

        Changes to this property won't take effect until the current connection, if open, is closed and a new connection opened.
        """
        return self._detect_types

    @detect_types.setter
    def detect_types(self, should_detect: bool):
        self._detect_types = should_detect

    @property
    def enforce_foreign_keys(self) -> bool:
        return self._enforce_foreign_keys

    @enforce_foreign_keys.setter
    def enforce_foreign_keys(self, should_enforce: bool):
        self._enforce_foreign_keys = should_enforce
        self._set_foreign_key_enforcement()

    @property
    def indicies(self) -> list[str]:
        """List of indicies for this database."""
        return [
            table["name"]
            for table in self.query(
                "SELECT name FROM sqlite_Schema WHERE type = 'index';"
            )
        ]

    @property
    def name(self) -> str:
        """The name of this database."""
        return self.path.stem

    @property
    def path(self) -> Pathier:
        """The path to this database file."""
        return self._path

    @path.setter
    def path(self, new_path: Pathish):
        """If `new_path` doesn't exist, it will be created (including parent folders)."""
        self._path = Pathier(new_path)
        if not self.path.exists():
            self.path.touch()

    @property
    def tables(self) -> list[str]:
        """List of table names for this database."""
        return [
            table["name"]
            for table in self.query(
                "SELECT name FROM sqlite_Schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%';"
            )
        ]

    @property
    def views(self) -> list[str]:
        """List of view for this database."""
        return [
            table["name"]
            for table in self.query(
                "SELECT name FROM sqlite_Schema WHERE type = 'view' AND name NOT LIKE 'sqlite_%';"
            )
        ]

    def _logger_init(self, log_path: Pathish | None = None):
        """:param: `message_format`: `{` style format string."""
        self.logger = loggi.getLogger(
            self.name, Pathier(log_path) if log_path else Pathier.cwd()
        )

    def _prepare_insert_queries(
        self, table: str, columns: Iterable[str], values: Sequence[Iterable[Any]]
    ) -> list[tuple[str, tuple[Any, ...]]]:
        """Format a list of insert statements.

        The returned value is a list because `values` will be broken up into chunks.

        Each list element is a two tuple consisting of the parameterized query string and a tuple of values.
        """
        inserts = []
        max_row_count = 900
        column_list = "(" + ", ".join(columns) + ")"
        for i in range(0, len(values), max_row_count):
            chunk = values[i : i + max_row_count]
            placeholder = (
                "(" + "),(".join((", ".join(("?" for _ in row)) for row in chunk)) + ")"
            )
            flattened_values = tuple((value for row in chunk for value in row))
            inserts.append(
                (
                    f"INSERT INTO {table} {column_list} VALUES {placeholder};",
                    flattened_values,
                )
            )
        return inserts

    def _set_foreign_key_enforcement(self):
        if self.connection:
            self.connection.execute(
                f"pragma foreign_keys = {int(self.enforce_foreign_keys)};"
            )

    def add_column(self, table: str, column_def: str):
        """Add a column to `table`.

        `column_def` should be in the form `{column_name} {type_name} {constraint}`.

        i.e.
        >>> db = Databased()
        >>> db.add_column("rides", "num_stops INTEGER NOT NULL DEFAULT 0")"""
        self.query(f"ALTER TABLE {table} ADD {column_def};")

    def close(self):
        """Disconnect from the database.

        Does not call `commit()` for you unless the `commit_on_close` property is set to `True`.
        """
        if self.connection:
            if self.commit_on_close:
                self.commit()
            self.connection.close()
            self.connection = None

    def commit(self):
        """Commit state of database."""
        if self.connection:
            self.connection.commit()
            self.logger.info("Committed successfully.")
        else:
            raise RuntimeError(
                "Databased.commit(): Can't commit db with no open connection."
            )

    def connect(self):
        """Connect to the database."""
        self.connection = sqlite3.connect(
            self.path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            if self.detect_types
            else 0,
            timeout=self.connection_timeout,
        )
        self._set_foreign_key_enforcement()
        self.connection.row_factory = dict_factory

    def count(
        self,
        table: str,
        column: str = "*",
        where: str | None = None,
        distinct: bool = False,
    ) -> int:
        """Return number of matching rows in `table` table.

        Equivalent to:
        >>> SELECT COUNT({distinct} {column}) FROM {table} {where};"""
        query = (
            f"SELECT COUNT( {('DISTINCT' if distinct else '')} {column}) FROM {table}"
        )
        if where:
            query += f" WHERE {where}"
        query += ";"
        return int(list(self.query(query)[0].values())[0])

    def create_table(self, table: str, *column_defs: str):
        """Create a table if it doesn't exist.

        #### :params:

        `table`: Name of the table to create.

        `column_defs`: Any number of column names and their definitions in proper Sqlite3 sytax.
        i.e. `"column_name TEXT UNIQUE"` or `"column_name INTEGER PRIMARY KEY"` etc."""
        columns = ", ".join(column_defs)
        result = self.query(f"CREATE TABLE IF NOT EXISTS {table} ({columns});")
        self.logger.info(f"'{table}' table created.")

    def delete(self, table: str, where: str | None = None) -> int:
        """Delete rows from `table` that satisfy the given `where` clause.

        If `where` is `None`, all rows will be deleted.

        Returns the number of deleted rows.

        e.g.
        >>> db = Databased()
        >>> db.delete("rides", "distance < 5 AND average_speed < 7")"""
        try:
            if where:
                self.query(f"DELETE FROM {table} WHERE {where};")
            else:
                self.query(f"DELETE FROM {table};")
            row_count = self.cursor.rowcount
            self.logger.info(
                f"Deleted {row_count} rows from '{table}' where '{where}'."
            )
            return row_count
        except Exception as e:
            self.logger.exception(
                f"Error deleting rows from '{table}' where '{where}'."
            )
            raise e

    def describe(self, table: str) -> list[dict]:
        """Returns information about `table`."""
        return self.query(f"pragma table_info('{table}');")

    def drop_column(self, table: str, column: str):
        """Drop `column` from `table`."""
        self.query(f"ALTER TABLE {table} DROP {column};")

    def drop_table(self, table: str) -> bool:
        """Drop `table` from the database.

        Returns `True` if successful, `False` if not."""
        try:
            self.query(f"DROP TABLE {table};")
            self.logger.info(f"Dropped table '{table}'.")
            return True
        except Exception as e:
            print(f"{type(e).__name__}: {e}")
            self.logger.error(f"Failed to drop table '{table}'.")
            return False

    def execute_script(self, path: Pathish, encoding: str = "utf-8") -> list[dict]:
        """Execute sql script located at `path`."""
        if not self.connected:
            self.connect()
        assert self.connection
        return self.connection.executescript(
            Pathier(path).read_text(encoding)
        ).fetchall()

    def get_columns(self, table: str) -> tuple[str, ...]:
        """Returns a list of column names in `table`."""
        return tuple(
            (column["name"] for column in self.query(f"pragma table_info('{table}');"))
        )

    def insert(
        self, table: str, columns: Iterable[str], values: Sequence[Iterable[Any]]
    ) -> int:
        """Insert rows of `values` into `columns` of `table`.

        Each `tuple` in `values` corresponds to an individual row that is to be inserted.
        """
        row_count = 0
        for insert in self._prepare_insert_queries(table, columns, values):
            try:
                self.query(insert[0], insert[1])
                row_count += self.cursor.rowcount
                self.logger.info(f"Inserted {row_count} rows into '{table}' table.")
            except Exception as e:
                self.logger.exception(f"Error inserting rows into '{table}' table.")
                raise e
        return row_count

    def query(self, query_: str, parameters: Sequence[Any] = tuple()) -> list[dict]:
        """Execute an SQL query and return the results.

        Ensures that the database connection is opened before executing the command.

        The cursor used to execute the query will be available through `self.cursor` until the next time `self.query()` is called.
        """
        if not self.connected:
            self.connect()
        assert self.connection
        self.cursor = self.connection.cursor()
        self.cursor.execute(query_, parameters)
        return self.cursor.fetchall()

    def rename_column(self, table: str, column_to_rename: str, new_column_name: str):
        """Rename a column in `table`."""
        self.query(
            f"ALTER TABLE {table} RENAME {column_to_rename} TO {new_column_name};"
        )

    def rename_table(self, table_to_rename: str, new_table_name: str):
        """Rename a table."""
        self.query(f"ALTER TABLE {table_to_rename} RENAME TO {new_table_name};")

    def select(
        self,
        table: str,
        columns: Iterable[str] = ["*"],
        joins: Iterable[str] | None = None,
        where: str | None = None,
        group_by: str | None = None,
        having: str | None = None,
        order_by: str | None = None,
        limit: int | str | None = None,
        exclude_columns: Iterable[str] | None = None,
    ) -> list[dict]:
        """Return rows for given criteria.

        If `exclude_columns` is given, `columns` will be ignored and data will be returned with all columns except the ones specified by `exclude_columns`.

        For complex queries, use the `databased.query()` method.

        Parameters `where`, `group_by`, `having`, `order_by`, and `limit` should not have
        their corresponding key word in their string, but should otherwise be valid SQL.

        `joins` should contain their key word (`INNER JOIN`, `LEFT JOIN`) in addition to the rest of the sub-statement.

        >>> Databased().select(
            "bike_rides",
            ["id", "date", "distance", "moving_time", "AVG(distance/moving_time) as average_speed"],
            where="distance > 20",
            order_by="distance",
            desc=True,
            limit=10
            )
        executes the query:
        >>> SELECT
                id, date, distance, moving_time, AVG(distance/moving_time) as average_speed
            FROM
                bike_rides
            WHERE
                distance > 20
            ORDER BY
                distance DESC
            Limit 10;"""
        # Assume implicit `[*]` for `columns` param when `exclude_columns` is used.
        if exclude_columns:
            columns = [
                column
                for column in self.get_columns(table)
                if column not in exclude_columns
            ]
        query = f"SELECT {', '.join(columns)} FROM {table}"
        if joins:
            query += f" {' '.join(joins)}"
        if where:
            query += f" WHERE {where}"
        if group_by:
            query += f" GROUP BY {group_by}"
        if having:
            query += f" HAVING {having}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"
        query += ";"
        rows = self.query(query)
        return rows

    @staticmethod
    def to_grid(data: Iterable[dict], shrink_to_terminal: bool = True) -> str:
        """Returns a tabular grid from `data`.

        If `shrink_to_terminal` is `True`, the column widths of the grid will be reduced to fit within the current terminal.
        """
        return griddy(data, "keys", shrink_to_terminal)

    def update(
        self, table: str, column: str, value: Any, where: str | None = None
    ) -> int:
        """Update `column` of `table` to `value` for rows satisfying the conditions in `where`.

        If `where` is `None` all rows will be updated.

        Returns the number of updated rows.

        e.g.
        >>> db = Databased()
        >>> db.update("rides", "elevation", 100, "elevation < 100")"""
        try:
            if where:
                self.query(f"UPDATE {table} SET {column} = ? WHERE {where};", (value,))
            else:
                self.query(f"UPDATE {table} SET {column} = ?;", (value,))
            row_count = self.cursor.rowcount
            self.logger.info(
                f"Updated {row_count} rows in '{table}' table to '{column}' = '{value}' where '{where}'."
            )
            return row_count
        except Exception as e:
            self.logger.exception(
                f"Failed to update rows in '{table}' table to '{column}' = '{value}' where '{where}'."
            )
            raise e

    def vacuum(self) -> int:
        """Reduce disk size of database after row/table deletion.

        Returns space freed up in bytes."""
        size = self.path.size
        self.query("VACUUM;")
        return size - self.path.size

    # Seat ========================== Database Dump =========================================

    def _format_column_def(self, description: dict) -> str:
        name = description["name"]
        type_ = description["type"]
        primary_key = bool(description["pk"])
        not_null = bool(description["notnull"])
        default = description["dflt_value"]
        column = f"{name} {type_}"
        if primary_key:
            column += f" PRIMARY KEY"
        if not_null:
            column += f" NOT NULL"
        if default:
            if isinstance(default, str):
                default = f"{default}"
            column += f" DEFAULT {default}"
        return column

    def _format_table_data(self, table: str) -> str:
        columns = self.get_columns(table)
        rows = [tuple(row.values()) for row in self.select(table)]
        inserts = self._prepare_insert_queries(table, columns, rows)
        insert_strings = []
        indent = " " * 4
        for insert in inserts:
            text = insert[0]
            sub = "^$data$based$^"
            text = text.replace("?", sub)
            for value in insert[1]:
                if not value:
                    value = ""
                if isinstance(value, bool):
                    value = int(value)
                if not isinstance(value, int) and (not isinstance(value, float)):
                    if isinstance(value, str):
                        value = value.replace('"', "'")
                    value = f'"{value}"'
                text = text.replace(sub, str(value), 1)
            for pair in [
                ("INSERT INTO ", f"INSERT INTO\n{indent}"),
                (") VALUES (", f")\nVALUES\n{indent}("),
                ("),", f"),\n{indent}"),
            ]:
                text = text.replace(pair[0], pair[1])
            insert_strings.append(text)
        return "\n".join(insert_strings)

    def _format_table_def(self, table: str) -> str:
        description = self.describe(table)
        indent = " " * 4
        columns = ",\n".join(
            (f"{indent * 2}{self._format_column_def(column)}" for column in description)
        )
        table_def = (
            "CREATE TABLE IF NOT EXISTS\n"
            + f"{indent}{table} (\n"
            + columns
            + f"\n{indent});"
        )
        return table_def

    def _get_data_dump_string(self, tables: Iterable[str]) -> str:
        return "\n\n".join((self._format_table_data(table) for table in tables))

    def _get_schema_dump_string(self, tables: Iterable[str]) -> str:
        return "\n\n".join((self._format_table_def(table) for table in tables))

    def dump_data(self, path: Pathish, tables: Iterable[str] | None = None):
        """Create a data dump file for the specified tables or all tables, if none are given."""
        tables = tables or self.tables
        path = Pathier(path)
        path.write_text(self._get_data_dump_string(tables), encoding="utf-8")

    def dump_schema(self, path: Pathish, tables: Iterable[str] | None = None):
        """Create a schema dump file for the specified tables or all tables, if none are given.

        NOTE: Foreign key relationships/constraints are not preserved when dumping the schema.
        """
        tables = tables or self.tables
        path = Pathier(path)
        path.write_text(self._get_schema_dump_string(tables), encoding="utf-8")
