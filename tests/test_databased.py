# type: ignore
from datetime import datetime

import pandas
import pytest
from pathier import Pathier

from databased import Databased

root = Pathier(__file__).parent
dummy_path = root / "dummy"
dbpath = dummy_path / "dummy.db"


def setup_module():
    ...


def teardown_module():
    dummy_path.delete()


def test__init():
    db = Databased(dbpath)
    assert db.path.exists()


def test__path():
    db = Databased(dbpath)
    assert db.path == dbpath


def test__name():
    db = Databased(dbpath)
    assert db.name == dbpath.stem


def test__connection():
    db = Databased(dbpath)
    assert db.connection is None
    db.connect()
    assert db.connection is not None
    db.disconnect()
    assert db.connection is None


def test__query():
    db = Databased(dbpath)
    db.query("SELECT * FROM sqlite_Schema;")
    db.disconnect()


def test__create_table():
    with Databased(dbpath) as db:
        db.create_table(
            "cereals",
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "name TEXT NOT NULL",
            "brand TEXT",
            "date_added TIMESTAMP",
        )
