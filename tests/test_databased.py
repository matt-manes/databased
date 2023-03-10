from datetime import datetime
from pathlib import Path

import pandas
import pytest

import databased

root = Path(__file__).parent
dbpath = root / "dummy" / "db.db"


def test_is_this_thing_on():
    dbpath.unlink(missing_ok=True)
    db = databased.DataBased(dbpath)
    assert db


def test__databased__create_table():
    with databased.DataBased(dbpath) as db:
        cols = ["name text", "favorite_int int", "date_added timestamp"]
        db.create_table("dummy", cols)
        db.create_table("dummy2", cols)


def test__databased___get_dict():
    ...


def test__databased___get_conditions():
    ...


def test__databased__get_table_names():
    with databased.DataBased(dbpath) as db:
        tables = db.get_table_names()
        assert "dummy" in tables
        assert "dummy2" in tables


def test__databased__get_column_names():
    with databased.DataBased(dbpath) as db:
        tables = db.get_table_names()
        for table in tables:
            cols = db.get_column_names(table)
            assert all(col in cols for col in ["name", "favorite_int", "date_added"])


def test__databased__add_row():
    with databased.DataBased(dbpath) as db:
        for row in [
            ("John Smith", 31, datetime.now()),
            ("Amy Gonzalez", 276, datetime.now()),
            ("Bobby Tables", 627, datetime.now()),
        ]:
            db.add_row("dummy", row)
        for row in [
            ("John Smith", 111, datetime.now()),
            ("Deshawn Sanders", 99, datetime.now()),
            ("Marsha Wallander", 31, datetime.now()),
        ]:
            db.add_row("dummy2", row)


def test__databased__count():
    with databased.DataBased(dbpath) as db:
        assert db.count("dummy") == 3
        assert db.count("dummy2", [("name", "Deshawn Sanders")]) == 1


def test__databased__get_rows():
    with databased.DataBased(dbpath) as db:
        rows = db.get_rows("dummy")
        assert len(rows) == 3
        assert all(col in rows[0] for col in ["name", "favorite_int", "date_added"])
        row = rows[0]
        assert type(row["name"]) == str
        assert type(row["favorite_int"]) == int
        assert type(row["date_added"]) == datetime
        assert row["name"] == "John Smith"
        assert row["favorite_int"] == 31
        assert row["date_added"] < datetime.now()
        rows = db.get_rows("dummy2", columns_to_return=["name"])
        assert len(rows[0]) == 1
        rows = db.get_rows("dummy2", columns_to_return=["name"], values_only=True)
        assert type(rows[0]) == tuple
        assert rows[0][0] == "John Smith"
        assert len(db.get_rows("dummy2", limit=1)) == 1
        assert (
            db.get_rows("dummy2", order_by="favorite_int")[0]["name"]
            == "Marsha Wallander"
        )
        df = db.get_rows("dummy2", return_as_dataframe=True)
        assert type(df) == pandas.DataFrame
        cols = db.get_column_names("dummy2")
        assert len(df.columns) == len(cols)
        for i in range(len(cols)):
            assert df.columns[i] == cols[i]
        rows = db.get_rows("dummy2", values_only=True)
        for row, df_row in zip(rows, df.values):
            for item1, item2 in zip(row, df_row):
                assert item1 == item2


def test__databased_query():
    with databased.DataBased(dbpath) as db:
        rows = db.query("select name from dummy2 where name like 'Deshawn Sanders';")
        assert len(rows) == 1
        assert rows[0][0] == "Deshawn Sanders"


def test__databased__find():
    ...


def test__databased__delete():
    ...


def test__databased__update():
    ...


def test__databased__drop_table():
    ...


def test__databased__add_column():
    ...


def test__databased__data_to_string():
    ...


def test__cleanup():
    for path in dbpath.parent.iterdir():
        path.unlink()
