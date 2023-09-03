import logging
import sqlite3
from functools import wraps
from typing import Any, Type

import pandas
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
        logger_encoding: str = "utf-8",
        logger_message_format: str = "{levelname}|-|{asctime}|-|{message}",
        detect_types: bool = True,
    ):
        """ """
        self.path = dbpath
        self.connection_timeout = connection_timeout
        self.connection = None
        self._logger_init(logger_message_format, logger_encoding)
        self.detect_types = detect_types

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args, **kwargs):
        self.commit()
        self.close()

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
    def detect_types(self) -> bool:
        """Should use `detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES` when establishing a database connection."""
        return self._detect_types

    @detect_types.setter
    def detect_types(self, should_detect: bool):
        self._detect_types = should_detect

    @property
    def name(self) -> str:
        """The name of this database."""
        return self.path.stem

    @property
    def connected(self) -> bool:
        """Whether this `Databased` instance is connected to the database file or not."""
        return self.connection is not None

    @property
    def connection_timeout(self) -> float:
        return self._connection_timeout

    @connection_timeout.setter
    def connection_timeout(self, timeout: float):
        self._connection_timeout = timeout

    def connect(self):
        """Connect to the database."""
        self.connection = sqlite3.connect(
            self.path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            if self.detect_types
            else 0,
            timeout=self.connection_timeout,
        )
        self.connection.execute("pragma foreign_keys = 1;")
        self.connection.row_factory = dict_factory

    def close(self):
        """Disconnect from the database. Does not call `commit()` for you."""
        if self.connection:
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

    def query(self, query_: str, parameters: tuple[Any] = tuple()) -> list[dict]:
        """Execute an SQL query and return the results.

        Ensures that the database connection is opened before executing the command.

        The cursor used to execute the query will be available through `self.cursor` until the next time `self.query()` is called."""
        if not self.connected:
            self.connect()
        assert self.connection
        self.cursor = self.connection.cursor()
        self.cursor.execute(query_, parameters)
        return self.cursor.fetchall()

    def execute_script(self, path: Pathish, encoding: str = "utf-8") -> sqlite3.Cursor:
        """Execute sql script located at `path`."""
        if not self.connected:
            self.connect()
        assert self.connection
        return self.connection.executescript(Pathier(path).read_text(encoding))

    def create_table(self, table: str, *column_defs: str):
        """Create a table if it doesn't exist.

        #### :params:

        `table`: Name of the table to create.

        `column_defs`: Any number of column names and their definitions in proper Sqlite3 sytax.
        i.e. `"column_name TEXT UNIQUE"` or `"column_name INTEGER PRIMARY KEY"` etc."""
        columns = ", ".join(column_defs)
        result = self.query(f"CREATE TABLE IF NOT EXISTS {table} ({columns});")
        self.logger.info(f"'{table}' table created.")
        return result

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

    @property
    def tables(self) -> list[str]:
        """List of table names for this database."""
        return [
            table["name"]
            for table in self.query(
                "SELECT name FROM sqlite_Schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%';"
            )
        ]

    def columns(self, table: str) -> list[str]:
        """Returns a list of column names in `table`."""
        return [
            column["name"] for column in self.query(f"pragma table_info('{table}');")
        ]

    def describe(self, table: str) -> list[dict]:
        """Returns information about `table`."""
        return self.query(f"pragma table_info('{table}');")

    def vacuum(self) -> int:
        """Reduce disk size of database after row/table deletion.

        Returns space freed up in bytes."""
        size = self.path.size
        self.query("VACUUM;")
        return size - self.path.size

    def insert(self, table: str, columns: tuple[str], values: tuple[Any]) -> int:
        """Insert `values` into `columns` of `table`."""
        placeholder = "(" + ", ".join("?" for _ in values) + ")"
        logger_values = "(" + ", ".join(str(value) for value in values) + ")"
        column_list = "(" + ", ".join(columns) + ")"
        try:
            self.query(
                f"INSERT INTO {table} {column_list} VALUES {placeholder};",
                values,
            )
            self.logger.info(
                f"Inserted '{logger_values}' into '{column_list}' columns of '{table}' table."
            )
            return self.cursor.rowcount
        except Exception as e:
            self.logger.exception(
                f"Error inserting '{logger_values}' into '{column_list}' columns of '{table}' table."
            )
            raise e

    def insert_many(
        self, table: str, columns: tuple[str], values: list[tuple[Any]]
    ) -> int:
        """Insert multiple rows of `values` into `columns` of `table`."""
        chunk_size = 900
        column_list = "(" + ", ".join(columns) + ")"
        row_count = 0
        for i in range(0, len(values), chunk_size):
            chunk = values[i : i + chunk_size]
            placeholder = (
                "(" + "),(".join(", ".join("?" for _ in row) for row in chunk) + ")"
            )
            logger_values = "\n".join(
                "'(" + ", ".join(str(value) for value in row) + ")'" for row in chunk
            )
            flattened_values = tuple(value for row in chunk for value in row)
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

    def select(
        self,
        table: str,
        columns: str = "*",
        joins: str | None = None,
        where: str | None = None,
        group_by: str | None = None,
        having: str | None = None,
        order_by: str | None = None,
        desc: bool = False,
        limit: int | None = None,
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
        query = f"SELECT {columns} FROM {table}"
        if joins:
            query += f" {joins}"
        if where:
            query += f" WHERE {where}"
        if group_by:
            query += f" GROUP BY {group_by}"
        if having:
            query += f" HAVING {having}"
        if order_by:
            query += f" ORDER BY {order_by}"
            if desc:
                query += " DESC"
        if limit:
            query += f" LIMIT {limit}"
        query += ";"
        rows = self.query(query)
        return rows
