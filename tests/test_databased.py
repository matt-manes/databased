# type: ignore
from datetime import datetime
from functools import partial
import pandas
import pytest
from pathier import Pathier

from databased import Databased

root = Pathier(__file__).parent
dummy_path = root / "dummy"
dbpath = dummy_path / "dummy.sqlite3"

DB = partial(Databased, dbpath)


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
    db.close()
    assert db.connection is None


def test__foreign_keys():
    db = DB()
    assert db.enforce_foreign_keys
    db.enforce_foreign_keys = False
    assert not db.enforce_foreign_keys
    db.enforce_foreign_keys = True
    assert db.enforce_foreign_keys


def test__query():
    db = DB()
    db.query("SELECT * FROM sqlite_Schema;")
    db.close()


def test__create_table():
    with DB() as db:
        db.create_table(
            "cereals",
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "name TEXT NOT NULL",
            "brand TEXT",
            "date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        )


def test__tables():
    with DB() as db:
        assert db.tables == ["cereals"]


def test__columns():
    with DB() as db:
        assert db.get_columns("cereals") == ["id", "name", "brand", "date_added"]


def test__insert():
    with DB() as db:
        db.insert(
            "cereals",
            ("name", "brand"),
            [
                ("Sugar Berries", "Super Health"),
                ("Shreddy Bois", "Dr. Wheat"),
                ("Chungus Os", "Chompers"),
                ("Soggy Crisps", "Wet Willys Wonders"),
            ],
        )


def test__select():
    with DB() as db:
        rows = db.select("cereals")
        print(rows)
        assert len(rows) == 4
        assert len(rows[0]) == 4
        rows = db.select("cereals", columns="name, brand", where="name LIKE 's%'")
        print(rows)
        assert len(rows) == 3
        assert len(rows[0]) == 2


def test__update():
    with DB() as db:
        assert db.update("cereals", "brand", "Big Gravy", "brand = 'Chompers'") == 1
        assert db.update("cereals", "brand", "Lockheed", "brand != 'Big Gravy'") == 3
        assert db.update("cereals", "brand", "littlegravy") == 4


def test__rename_table():
    with DB() as db:
        db.rename_table("cereals", "serials")
        assert "serials" in db.tables and "cereals" not in db.tables
        db.rename_table("serials", "cereals")


def test__rename_column():
    with DB() as db:
        db.rename_column("cereals", "brand", "company")
        assert "company" in db.get_columns("cereals") and "brand" not in db.get_columns(
            "cereals"
        )
        db.rename_column("cereals", "company", "brand")


def test__add_column():
    with DB() as db:
        db.add_column("cereals", "sugar_content INTEGER NOT NULL DEFAULT 10000")
        assert "sugar_content" in db.get_columns("cereals")


def test__drop_column():
    with DB() as db:
        assert "sugar_content" in db.get_columns("cereals")
        db.drop_column("cereals", "sugar_content")
        assert "sugar_content" not in db.get_columns("cereals")


def test__delete():
    with DB() as db:
        assert db.delete("cereals", "name = 'Shreddy Bois'") == 1
        assert db.delete("cereals") == 3
