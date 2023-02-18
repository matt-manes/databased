# Databased
Databased is a package that wraps the standard library Sqlite3 module to make setting up and using a database quicker and easier.<br>
Install with:
<pre>pip install databased</pre>
databased is a package that wraps the standard library Sqlite3 module to largely avoid writing queries except for table definitions.<br>
It consists of the class DataBased and an additional function for displaying information in a grid called data_to_string.<br>
The DataBased class contains functions for creating databases and tables; inserting, updating, and deleting rows; 
as well as retrieving data and schema information.<br>
The data_to_string function uses the [tabulate](https://pypi.org/project/tabulate/) to generate a grid as a string from a list of dictionaries.<br>
By default, data_to_string will automatically wrap the width of columns to fit within the current terminal window.<br><br>
Member functions that require a database connection will
automatically create one when called if one isn't already open,
but a manual call to self.close() needs to be called in order to
save the database file and release the connection.<br>
If a context manager is used, like in the following example, you don't need to worry about manually opening, saving, or closing the database.<br>
<br>
Usage:
<pre>
from databased import DataBased, data_to_string
from datetime import datetime

# if the .db file specified doesn't exist, it will be created
# a log file with the same name will be generated and stored in the same directory
with DataBased(dbpath="records.db") as db:
    tables = [
        "kitchen_tables(num_legs int, top_material text, shape text, date_added timestamp)"
    ]
    # A table will only be created if it doesn't exist. create_tables() will not overwrite an existing table.
    db.create_tables(tables)
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
    print(data_to_string(db.get_rows("kitchen_tables"), sort_key="top_material"))
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
and the final print() call on the data_to_string() function produces:
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
When a DataBased object is created, if there is no file named "dbmanager.py"
in the same directory as the specified database file, one will be created from
a template.<br>
"dbmanager.py" is a command line script that provides basic database commands,
but can be tailored to a given database(s).<br>
Instead of invoking the script over and over with new commands,
the script is invoked once and will repetedly prompt the user for commands.<br>
Invoking the script and then passing the "-h/--help" command:
<pre>
>dbmanager.py
Enter command: -h
usage: dbmanager.py [-h] [-db DB_NAME] [-i] [-t [TABLES ...]] [-c [COLUMNS ...]] [-f FIND] [-sco] [-d [DELETE ...]] [-u UPDATE UPDATE] [-sb SORT_BY]

options:
  -h, --help            show this help message and exit
  -db DBNAME, --dbname DBNAME
                        Name of database file to use. Required on the first loop if no default is set, but subsequent loops will resuse the same database unless a new one is provided through this arg.
  -i, --info            Display table names, their respective columns, and how many records they contain. If a -t/--tables arg is passed, just the columns and row count for those tables will be shown.
  -t [TABLES ...], --tables [TABLES ...]
                        Limits commands to a specific list of tables. Optional for some commands, required for others. If this is the only arg given (besides -db if not already set), the whole table will be printed to the terminal.
  -c [COLUMNS ...], --columns [COLUMNS ...]
                        Limits commands to a specific list of columns. Optional for some commands, required for others. If this and -t are the only args given (besides -db if not already set), the whole table will be printed to the
                        terminal, but with only the columns provided with this arg.
  -f FIND, --find FIND  A substring to search the database for. If a -c/--columns arg(s) is not given, the values will be matched against all columns. Similarly, if a -t/--tables arg(s) is not given, the values will be searched for in
                        all tables.
  -sco, --show_count_only
                        Show the number of results returned by -f/--find, but don't print the results to the terminal.
  -d [DELETE ...], --delete [DELETE ...]
                        A list of values to be deleted from the database. A -c/--columns arg must be supplied. A -t/--tables arg must be supplied.
  -u UPDATE UPDATE, --update UPDATE UPDATE
                        Update a record in the database. Expects two arguments: the current value and the new value. A -c/--columns arg must be supplied. A -t/--tables arg must be supplied.
  -sb SORT_BY, --sort_by SORT_BY
                        Column to sort results by.
Enter command:
</pre>