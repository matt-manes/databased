# type: ignore
import pytest
from pathier import Pathier

from databased import Databased

root = Pathier(__file__).parent


@pytest.fixture(scope="module")
def dbpath(tmp_path_factory) -> Pathier:
    dummy_path = Pathier(tmp_path_factory.mktemp("dummy"))
    dummy_path.mkcwd()
    return dummy_path / "dummy.sqlite3"


@pytest.fixture(scope="module")
def db(dbpath: Pathier) -> Databased:
    return Databased(dbpath)


def test__init(dbpath: Pathier):
    db = Databased(dbpath)
    assert db.path.exists()


def test__path(dbpath: Pathier):
    db = Databased(dbpath)
    assert db.path == dbpath


def test__name(dbpath: Pathier):
    db = Databased(dbpath)
    assert db.name == dbpath.stem


def test__connection(db: Databased):
    assert db.connection is None
    db.connect()
    assert db.connection is not None
    db.close()
    assert db.connection is None


def test__foreign_keys(db: Databased):
    assert db.enforce_foreign_keys
    db.enforce_foreign_keys = False
    assert not db.enforce_foreign_keys
    db.enforce_foreign_keys = True
    assert db.enforce_foreign_keys


def test__query(db: Databased):
    db.query("SELECT * FROM sqlite_Schema;")
    db.close()


def test__create_table(db: Databased):
    with db as db:
        db.create_table(
            "cereals",
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "name TEXT NOT NULL",
            "brand TEXT",
            "date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        )


def test__tables(db: Databased):
    with db as db:
        assert db.tables == ["cereals"]


def test__columns(db: Databased):
    with db as db:
        assert db.get_columns("cereals") == ("id", "name", "brand", "date_added")


def test__insert(db: Databased):
    with db as db:
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


def test__select(db: Databased):
    with db as db:
        rows = db.select("cereals")
        print(rows)
        assert len(rows) == 4
        assert len(rows[0]) == 4
        rows = db.select("cereals", columns=["name", "brand"], where="name LIKE 's%'")
        print(rows)
        assert len(rows) == 3
        assert len(rows[0]) == 2


def test__update(db: Databased):
    with db as db:
        assert db.update("cereals", "brand", "Big Gravy", "brand = 'Chompers'") == 1
        assert db.update("cereals", "brand", "Lockheed", "brand != 'Big Gravy'") == 3
        assert db.update("cereals", "brand", "littlegravy") == 4


def test__rename_table(db: Databased):
    with db as db:
        db.rename_table("cereals", "serials")
        assert "serials" in db.tables and "cereals" not in db.tables
        db.rename_table("serials", "cereals")


def test__rename_column(db: Databased):
    with db as db:
        db.rename_column("cereals", "brand", "company")
        assert "company" in db.get_columns("cereals") and "brand" not in db.get_columns(
            "cereals"
        )
        db.rename_column("cereals", "company", "brand")


def test__add_column(db: Databased):
    with db as db:
        db.add_column("cereals", "sugar_content INTEGER NOT NULL DEFAULT 10000")
        assert "sugar_content" in db.get_columns("cereals")


def test__drop_column(db: Databased):
    with db as db:
        assert "sugar_content" in db.get_columns("cereals")
        db.drop_column("cereals", "sugar_content")
        assert "sugar_content" not in db.get_columns("cereals")


def test__count(db: Databased):
    assert db.count("cereals") == 4
    assert db.count("cereals", where="name LIKE '%s'")


def test__logpath(dbpath: Pathier):
    new_log_path = dbpath.parent / "new_logs"
    assert not new_log_path.exists()
    with Databased(dbpath, log_dir=new_log_path) as db:
        pass
    assert new_log_path.exists()


def test__column_excludes(db: Databased):
    with db as db:
        row = db.select("cereals", exclude_columns=["date_added"])[0]
        assert len(row.keys()) == 3
        assert "date_added" not in row


def test__delete(db: Databased):
    # This test needs to be last
    with db as db:
        assert db.delete("cereals", "name = 'Shreddy Bois'") == 1
        assert db.delete("cereals") == 3
