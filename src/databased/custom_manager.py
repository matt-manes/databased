from argshell import ArgshellParser, Namespace, with_parser

from databased import DataBased, DBManager, dbparsers


class CustomManager(DBManager):
    intro = "Starting custommanager (enter help or ? for command info)..."
    prompt = "custommanager>"
    dbpath = None  # Replace None with a path to a .db file to set a default database


# For help with adding custom functionality see:
# https://github.com/matt-manes/argshell
# https://github.com/matt-manes/databased/blob/main/src/databased/dbmanager.py
# https://github.com/matt-manes/databased/blob/main/src/databased/dbparsers.py

if __name__ == "__main__":
    CustomManager().cmdloop()
