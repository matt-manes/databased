# Databased
Databased is a package that wraps the standard library Sqlite3 module to make setting up and using a database quicker and easier.<br>

Install with:
<pre>pip install databased</pre>

`databased` is a package that wraps the standard library Sqlite3 module to largely avoid writing queries except for table definitions (there is a `query` function that can be used to directly excute queries).<br>
It primarily consists of the class `DataBased`.<br>
The DataBased class contains functions for creating databases and tables; inserting, updating, and deleting rows; 
as well as retrieving data and schema information.<br>

There is also a static method called `data_to_string` which uses [tabulate](https://pypi.org/project/tabulate/) to generate a printable grid from a list of dictionaries.<br>
By default, data_to_string will automatically wrap the width of columns to fit within the current terminal window.<br><br>
Member functions that require a database connection will automatically create one when called if one isn't already open.<br>
If you create a class that inherits from `DataBased`, you will need to decorate member functions that access the database with `@_connect`.<br>
Unless `DataBased` is used with a context manager, you will need to manually call the `close()` function to close the connection and save the database.
<br>
Usage:
<pre>
from databased import DataBased
from datetime import datetime

# if the .db file specified doesn't exist, it will be created
# a log file with the same name will be generated and stored in the same directory
with DataBased(dbpath="records.db") as db:
    # A table will only be created if it doesn't exist. create_tables() will not overwrite an existing table.
    db.create_table("kitchen_tables", ["num_legs int", "top_material text", "shape text", "date_added timestamp"])
    kitchen_tables = [
        (4, "birch", "round", datetime.now()),
        (3, "oak", "round", datetime.now()),
        (6, "granite", "rectangle", datetime.now()),
    ]
    for kitchen_table in kitchen_tables:
        db.add_row("kitchen_tables", kitchen_table)

    print(f'number of rows: {db.count("kitchen_tables")}')
    print(f'table names: {db.get_table_names()}')
    print(f'column names: {db.get_column_names("kitchen_tables")}')
    print(db.get_rows("kitchen_tables", [("num_legs", 6)]))
    print(db.get_rows("kitchen_tables", [("shape", "round")], sort_by_column="num_legs"))
    print(db.get_rows("kitchen_tables", [("shape", "round"), ("num_legs", 4)]))

    db.update(
        "kitchen_tables",
        column_to_update="top_material",
        new_value="glass",
        match_criteria=[("num_legs", 3)],
    )
    print(db.get_rows("kitchen_tables", sort_by_column="num_legs"))
    print(db.data_to_string(db.get_rows("kitchen_tables"), sort_key="top_material"))
</pre>
produces:
<pre>
number of rows: 3
table names: ['kitchen_tables']
column names: ['num_legs', 'top_material', 'shape', 'date_added']
[{'num_legs': 6, 'top_material': 'granite', 'shape': 'rectangle', 'date_added': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}]
[{'num_legs': 3, 'top_material': 'oak', 'shape': 'round', 'date_added': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}, {'num_legs': 4, 'top_material': 'birch', 'shape': 'round', 'date_added': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}]
[{'num_legs': 4, 'top_material': 'birch', 'shape': 'round', 'date_added': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}]
[{'num_legs': 3, 'top_material': 'glass', 'shape': 'round', 'date_added': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}, {'num_legs': 4, 'top_material': 'birch', 'shape': 
'round', 'date_added': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}, {'num_legs': 6, 'top_material': 'granite', 'shape': 'rectangle', 'date_added': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}]
</pre>
and the final `print()` call on `data_to_string()` produces:
<pre>
+------------+----------------+-----------+-----------------------------+
| num_legs   | top_material   | shape     | date_added                  |
+============+================+===========+=============================+
| 4          | birch          | round     | 2022-12-14 18:19:31.501745  |
+------------+----------------+-----------+-----------------------------+
| 3          | glass          | round     | 2022-12-14 18:19:31.501745  |
+------------+----------------+-----------+-----------------------------+
| 6          | granite        | rectangle | 2022-12-14 18:19:31.501745  |
+------------+----------------+-----------+-----------------------------+
</pre>

`databased` also comes with an interactive shell called `dbmanager`, which is built from the [argshell](https://github.com/matt-manes/argshell) package.<br>
It can be launched from the terminal by entering `dbmanager`
<pre>
databased\tests>dbmanager
Searching for database...
DB options:
(1) test.db (2) test_bckup.db
Enter the number of the option to use: 1
Starting dbmanager (enter help or ? for arg info)...
based>help

Documented commands (type help {topic}):
========================================
add_column  backup     dbpath      flush_log  query     search  sys
add_row     count      delete      help       quit      show    update
add_table   customize  drop_table  info       scan_dbs  size    use_db

>based help update
Update a column to a new value.
        Two required positional args: the column to update and the value to update to.
        Use the -t/--tables flag to limit what tables are updated.
        Use the -m/--match_pairs flag to specify which rows are updated.
        >>> based>update username big_chungus -t users -m username lil_chungus

        ^will update the username in the users 'table' to 'big_chungus' where the username is currently 'lil_chungus'^
Parser help for update:
usage: dbmanager [-h] [-t [TABLES ...]] [-m [MATCH_PAIRS ...]] [-p] -c COLUMN -v NEW_VALUE

options:
  -h, --help            show this help message and exit
  -t [TABLES ...], --tables [TABLES ...]
                        Limits command to a specific list of tables
  -m [MATCH_PAIRS ...], --match_pairs [MATCH_PAIRS ...]
                        Pairs of columns and values to use for narrowing the scope of row operations. i.e. 'find -t users -m name Bob state Alaska last_login *' will print all rows from the users table that have the name Bob, are from the state Alaska, and last logged in at any
                        date.
  -p, --partial_matching
                        When selecting rows using a string, the string can be a substring instead of an exact match. i.e. "-t names -m first theo" only returns rows from names where the first name is exactly 'theo'. "-t names -m first theo -p" would return rows with first names
                        of 'theo', but also rows with names like 'theodore'.
  -c COLUMN, --column COLUMN
                        The column to update.
  -v NEW_VALUE, --new_value NEW_VALUE
                        The new value to update with.
based>
</pre>
The `customize` command can be used to generate a template file in the current directory that subclasses `DBManager`.<br>
This allows for project specific additional commands as well as modifications of available commands.



