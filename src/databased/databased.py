import logging
import sqlite3
from typing import Any

from griddle import griddy
from pathier import Pathier, Pathish


def dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    fields = [column[0] for column in cursor.description]
    return {column: value for column, value in zip(fields, row)}


class Databased:
    """SQLite3 wrapper."""

    def __init__(
        self,
        dbpath: Pathish = "db.sqlite3",
        connection_timeout: float = 10,
        detect_types: bool = True,
        enforce_foreign_keys: bool = True,
        commit_on_close: bool = True,
        logger_encoding: str = "utf-8",
        logger_message_format: str = "{levelname}|-|{asctime}|-|{message}",
    ):
        """ """
        self.path = dbpath
        self.connection_timeout = connection_timeout
        self.connection = None
        self._logger_init(logger_message_format, logger_encoding)
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

    def _logger_init(self, message_format: str, encoding: str):
        """:param: `message_format`: `{` style format string."""
        self.logger = logging.getLogger(self.name)
        if not self.logger.hasHandlers():
            handler = logging.FileHandler(
                str(self.path).replace(".", "") + ".log", encoding=encoding
            )
            handler.setFormatter(
                logging.Formatter(
                    message_format, style="{", datefmt="%m/%d/%Y %I:%M:%S %p"
                )
            )
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

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
        script = Pathier(path).read_text(encoding).replace("\n", " ")
        return self.query(script)

    def get_columns(self, table: str) -> tuple[str]:
        """Returns a list of column names in `table`."""
        return tuple(
            column["name"] for column in self.query(f"pragma table_info('{table}');")
        )

    def insert(
        self, table: str, columns: tuple[str, ...], values: list[tuple[Any, ...]]
    ) -> int:
        """Insert rows of `values` into `columns` of `table`.

        Each `tuple` in `values` corresponds to an individual row that is to be inserted.
        """
        max_row_count = 900
        column_list = "(" + ", ".join(columns) + ")"
        row_count = 0
        for i in range(0, len(values), max_row_count):
            chunk = values[i : i + max_row_count]
            placeholder = (
                "(" + "),(".join((", ".join(("?" for _ in row)) for row in chunk)) + ")"
            )
            logger_values = "\n".join(
                (
                    "'(" + ", ".join((str(value) for value in row)) + ")'"
                    for row in chunk
                )
            )
            flattened_values = tuple((value for row in chunk for value in row))
            try:
                self.query(
                    f"INSERT INTO {table} {column_list} VALUES {placeholder};",
                    flattened_values,
                )
                self.logger.info(
                    f"Inserted into '{column_list}' columns of '{table}' table values \n{logger_values}."
                )
                row_count += self.cursor.rowcount
            except Exception as e:
                self.logger.exception(
                    f"Error inserting into '{column_list}' columns of '{table}' table values \n{logger_values}."
                )
                raise e
        return row_count

    def query(self, query_: str, parameters: tuple[Any, ...] = tuple()) -> list[dict]:
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
        columns: list[str] = ["*"],
        joins: list[str] | None = None,
        where: str | None = None,
        group_by: str | None = None,
        having: str | None = None,
        order_by: str | None = None,
        limit: int | str | None = None,
    ) -> list[dict]:
        """Return rows for given criteria.

        For complex queries, use the `databased.query()` method.

        Parameters `where`, `group_by`, `having`, `order_by`, and `limit` should not have
        their corresponding key word in their string, but should otherwise be valid SQL.

        `joins` should contain their key word (`INNER JOIN`, `LEFT JOIN`) in addition to the rest of the sub-statement.

        >>> Databased().select(
            "bike_rides",
            "id, date, distance, moving_time, AVG(distance/moving_time) as average_speed",
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
    def to_grid(data: list[dict], shrink_to_terminal: bool = True) -> str:
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
