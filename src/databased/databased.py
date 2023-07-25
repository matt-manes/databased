import logging
import sqlite3
from functools import wraps
from typing import Any

import pandas
from griddle import griddy
from pathier import Pathier, Pathish


class Databased:
    """SQLite3 wrapper."""

    def __init__(
        self,
        dbpath: Pathish,
        connection_timeout: float = 10,
        logger_encoding: str = "utf-8",
        logger_message_format: str = "{levelname}|-|{asctime}|-|{message}",
    ):
        """ """
        self.path = dbpath
        self.connection_timeout = connection_timeout
        self.connection = None
        self._logger_init(logger_message_format, logger_encoding)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args, **kwargs):
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
            self.path.mkdir()

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

    def connect(self, row_factory: Any = sqlite3.Row):
        """Connect to the database."""
        self.connection = sqlite3.connect(
            self.path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            timeout=self.connection_timeout,
        )
        self.connection.execute("pragma foreign_keys = 1;")
        self.connection.row_factory = row_factory

    def disconnect(self):
        """Disconnect from the database."""
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

    def close(self):
        """Commit the database and then disconnect."""
        self.commit()
        self.disconnect()

    def _logger_init(self, message_format: str, encoding: str):
        """:param `message_format`: '{' style format string"""
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
