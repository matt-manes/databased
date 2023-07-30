# type: ignore
from datetime import datetime
from functools import partial
import pandas
import pytest
from pathier import Pathier

from databased import Databased

root = Pathier(__file__).parent
dummy_path = root / "dummy"
dbpath = dummy_path / "dummy.db"

DB = partial(Databased, dbpath)


def setup_module():
    ...


def teardown_module():
    dummy_path.delete()


def test__init():
    db = DB()
    assert db.path.exists()


def test__path():
    db = DB()
    assert db.path == dbpath


def test__name():
    db = DB()
    assert db.name == dbpath.stem


def test__connection():
    db = DB()
    assert db.connection is None
    db.connect()
    assert db.connection is not None
    db.disconnect()
    assert db.connection is None


def test__query():
    db = DB()
    db.query("SELECT * FROM sqlite_Schema;")
    db.disconnect()


def test__create_table():
    with DB() as db:
        db.create_table(
            "cereals",
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "name TEXT NOT NULL",
            "brand TEXT",
            "date_added TIMESTAMP",
        )
