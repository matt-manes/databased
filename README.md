# Databased

Databased is a module that wraps the standard library SQLite3 module to streamline integrating a database into a project.  

Install with:
```pip install databased```

## Usage

### Creation and Connections

```python
from databased import Databased

# for reference, "chi.db" is database of Chicago food inspections and business licenses
db = Databased("chi.db") # The file will be created if it doesn't exist
```

You can call `db.connect()` manually, but generally you shouldn't need to.  
All database functions are built on the `db.query()` function, which will open a connection if one isn't already established.  
e.g. Accessing the `db.tables` property uses the `db.query()` function and opens a connection for you

```python
print(*db.tables, sep="\n")
```

Output:

```console
business_addresses
businesses
license_codes
license_statuses
licenses
facility_types
risk_levels
facility_addresses
inspected_businesses
inspection_types
result_types
inspections
violation_types
violations
```

`db.query()` executes SQL strings and returns the results as a list of dictionaries

```python
print(
    *db.query("SELECT * FROM businesses WHERE legal_name LIKE 'z%' LIMIT 5;"), sep="\n"
)
```

Output:

```json
{'account_number': 106, 'legal_name': 'Zaven, Inc.', 'dba': 'Zaven / Lepetit Paris', 'address_id': 6880}
{'account_number': 113, 'legal_name': 'Zanies Comedy Clubs, Inc.', 'dba': 'Zanies Comedy Club', 'address_id': 5702}
{'account_number': 122, 'legal_name': 'Ziemek Corporation, Inc.', 'dba': 'The Thirsty Tavern', 'address_id': 146144}
{'account_number': 1918, 'legal_name': 'Zimmies Inc #8', 'dba': 'Original Pancake House', 'address_id': 143541}
{'account_number': 3007, 'legal_name': 'Zikainan Nursing Home Inc', 'dba': 'All American Nursing Home', 'address_id': 155957}
```

When a connection is no longer needed, it will need to be manually closed.

```python
db.close()
```

By default the database will be committed when db.close() is called.  
This can be prevented by setting `commit_on_close` to `False` in either the Databased constructor or through the property `db.commit_on_close`.  

```python
db = Databased("chi.db", commit_on_close=False)
# or
db.commit_on_close = False
```

The database can always be committed manually with `db.commit()`.  

Using Databased with a context manager will call the close() method for you (and commit the database if `commit_on_close` is `True`)  

```python
with Databased("chi.db") as db:
    print(f"{db.connected=}")
    print(*db.get_columns("businesses"), sep="\n")
print(f"{db.connected=}")
```

Output:

```python
db.connected=True
account_number
legal_name
dba
address_id
db.connected=False
```

### Tables and Columns

```python
with Databased("chi.db") as db:
    db.create_table(
        "inspectors",
        "id INTEGER PRIMARY KEY AUTOINCREMENT",
        "first_name TEXT",
        "last_name TEXT",
        "ward INTEGER",
    )
    db.insert(
        "inspectors",
        ("first_name", "last_name", "ward"),
        [("Billy", "Bob", 1), ("Jenna", "Jones", 33), ("Tiny", "Tim", 25)],
    )
    print(db.to_grid(db.select("inspectors")))
    print()
    # ---------------------------------------------------------
    db.rename_table("inspectors", "employees")
    db.add_column("employees", "title TEXT DEFAULT inspector")
    print(db.to_grid(db.select("employees")))
    print()
    # ---------------------------------------------------------
    db.drop_column("employees", "title")
    db.rename_table("employees", "inspectors")
    print(db.to_grid(db.select("inspectors")))
    print()
    db.drop_table("inspectors")
```

Output:

```console
+------+--------------+-------------+--------+
| id   | first_name   | last_name   | ward   |
+======+==============+=============+========+
| 1    | Billy        | Bob         | 1      |
+------+--------------+-------------+--------+
| 2    | Jenna        | Jones       | 33     |
+------+--------------+-------------+--------+
| 3    | Tiny         | Tim         | 25     |
+------+--------------+-------------+--------+

+------+--------------+-------------+--------+-----------+
| id   | first_name   | last_name   | ward   | title     |
+======+==============+=============+========+===========+
| 1    | Billy        | Bob         | 1      | inspector |
+------+--------------+-------------+--------+-----------+
| 2    | Jenna        | Jones       | 33     | inspector |
+------+--------------+-------------+--------+-----------+
| 3    | Tiny         | Tim         | 25     | inspector |
+------+--------------+-------------+--------+-----------+

+------+--------------+-------------+--------+
| id   | first_name   | last_name   | ward   |
+======+==============+=============+========+
| 1    | Billy        | Bob         | 1      |
+------+--------------+-------------+--------+
| 2    | Jenna        | Jones       | 33     |
+------+--------------+-------------+--------+
| 3    | Tiny         | Tim         | 25     |
+------+--------------+-------------+--------+
```

### Select

Moderately complex queries can be executed with `db.select()`.  
More advanced queries will need to be written out and executed directly with `db.query()`.  
Example using all available `db.select()` parameters:

```python
with Databased("chi.db") as db:
    print(
        db.to_grid(
            db.select(
                table="inspections",
                columns=[
                    "inspections.license_number",
                    "businesses.legal_name",
                    "result_types.id",
                    "result_types.description",
                    "business_addresses.ward",
                    "COUNT(*) AS num_inspections",
                ],
                joins=[
                    "INNER JOIN result_types ON inspections.result_type_id = result_types.id",
                    "INNER JOIN licenses ON inspections.license_number = licenses.license_number",
                    "INNER JOIN businesses ON licenses.account_number = businesses.account_number",
                    "INNER JOIN business_addresses ON businesses.address_id = business_addresses.id",
                ],
                where="business_addresses.ward IN (1, 10, 20, 40) OR result_types.id < 5",
                group_by="inspections.license_number",
                having="num_inspections > 10",
                order_by="num_inspections DESC",
                limit=5,
            )
        )
    )
    print("<==equivalent==>")
    print(
        db.to_grid(
            db.query(
                """
            SELECT 
            inspections.license_number, businesses.legal_name, 
            result_types.id, result_types.description, 
            business_addresses.ward, COUNT(*) as num_inspections
            FROM inspections
            INNER JOIN result_types ON inspections.result_type_id = result_types.id
            INNER JOIN licenses ON inspections.license_number = licenses.license_number
            INNER JOIN businesses ON licenses.account_number = businesses.account_number
            INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
            WHERE business_addresses.ward IN (1, 10, 20, 40) OR result_types.id < 5
            GROUP BY inspections.license_number
            HAVING num_inspections > 10
            ORDER BY num_inspections DESC
            LIMIT 5;
            """
            )
        )
```

Output:

```console
+------------------+-------------------------+------+--------------------+--------+-------------------+
| license_number   | legal_name              | id   | description        | ward   | num_inspections   |
+==================+=========================+======+====================+========+===================+
| 2583423          | Meadowflour Llc         | 7    | Pass W/ Conditions | 40     | 18                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
| 2594606          | Hz Ops Holdings Inc     | 4    | Not Ready          | 6      | 17                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
| 2405951          | Wendy's Properties, Llc | 7    | Pass W/ Conditions | 10     | 17                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
| 833              | Steve Ziemek            | 7    | Pass W/ Conditions | 10     | 17                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
| 55418            | 2053 W. Division Inc.   | 5    | Out Of Business    | 1      | 16                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
<==equivalent==>
+------------------+-------------------------+------+--------------------+--------+-------------------+
| license_number   | legal_name              | id   | description        | ward   | num_inspections   |
+==================+=========================+======+====================+========+===================+
| 2583423          | Meadowflour Llc         | 7    | Pass W/ Conditions | 40     | 18                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
| 2594606          | Hz Ops Holdings Inc     | 4    | Not Ready          | 6      | 17                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
| 2405951          | Wendy's Properties, Llc | 7    | Pass W/ Conditions | 10     | 17                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
| 833              | Steve Ziemek            | 7    | Pass W/ Conditions | 10     | 17                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
| 55418            | 2053 W. Division Inc.   | 5    | Out Of Business    | 1      | 16                |
+------------------+-------------------------+------+--------------------+--------+-------------------+
```

### Update

```python
with Databased("chi.db", commit_on_close=False) as db:
    print(db.to_grid(db.select("businesses", where="dba LIKE 'deli %'")))
    num_rows = db.update(
        "businesses", "dba", "deli BreadMeat City", "dba LIKE 'deli %'"
    )
    print(f"num rows updated: {num_rows}")
    print(db.to_grid(db.select("businesses", where="dba LIKE 'deli %'")))
```

Output:

```console
+------------------+-------------------------+---------------------------------+--------------+
| account_number   | legal_name              | dba                             | address_id   |
+==================+=========================+=================================+==============+
| 17214            | Emil Korpacki, Inc.     | Deli On Rice                    | 110271       |
+------------------+-------------------------+---------------------------------+--------------+
| 348935           | Deli King Inc.          | Deli King Inc.                  | 17090        |
+------------------+-------------------------+---------------------------------+--------------+
| 389169           | Pheidias, Inc.          | Deli Boutique, Wine And Spirits | 138721       |
+------------------+-------------------------+---------------------------------+--------------+
| 391766           | Ted's Deli & More, Inc. | Deli & More                     | 142080       |
+------------------+-------------------------+---------------------------------+--------------+
| 421955           | Deli Flavor, Inc.       | Deli Flavor                     | 5057         |
+------------------+-------------------------+---------------------------------+--------------+
num rows updated: 5
+------------------+-------------------------+---------------------+--------------+
| account_number   | legal_name              | dba                 | address_id   |
+==================+=========================+=====================+==============+
| 17214            | Emil Korpacki, Inc.     | deli BreadMeat City | 110271       |
+------------------+-------------------------+---------------------+--------------+
| 348935           | Deli King Inc.          | deli BreadMeat City | 17090        |
+------------------+-------------------------+---------------------+--------------+
| 389169           | Pheidias, Inc.          | deli BreadMeat City | 138721       |
+------------------+-------------------------+---------------------+--------------+
| 391766           | Ted's Deli & More, Inc. | deli BreadMeat City | 142080       |
+------------------+-------------------------+---------------------+--------------+
| 421955           | Deli Flavor, Inc.       | deli BreadMeat City | 5057         |
+------------------+-------------------------+---------------------+--------------+
```

### Delete

```python
with Databased("chi.db", commit_on_close=False) as db:
    num_rows = db.delete("businesses", "dba LIKE 'deli %' AND address_id > 6000")
    print(f"num rows deleted: {num_rows}")
    print(db.to_grid(db.select("businesses", where="dba LIKE 'deli %'")))
```

Output:

```console
num rows deleted: 4
+------------------+-------------------+-------------+--------------+
| account_number   | legal_name        | dba         | address_id   |
+==================+===================+=============+==============+
| 421955           | Deli Flavor, Inc. | Deli Flavor | 5057         |
+------------------+-------------------+-------------+--------------+
```

`databased` also comes with an interactive shell called `dbshell`, which is built from the [argshell](https://github.com/matt-manes/argshell) package.  
It can be launched from the terminal by entering `dbshell`

```console
>dbshell
Searching for database...
Could not find a .db file in e:/1vsCode/python/databased.
Enter path to .db file to use or press enter to search again recursively:
Searching recursively...
DB options:
(1) shelltesting/chi.db (2) shelltesting/chi_backup.db (3) shelltesting/chi_backup_09-21-2023-12_06_37_PM.db
Enter the number of the option to use: 1
Starting dbshell v3.0.0 (enter help or ? for arg info)...

chi.db>help

Documented commands (type help <topic>):
========================================
add_column  describe     properties  schema                    size
backup      drop_column  query       select                    sys
customize   drop_table   quit        set_connection_timeout    update
dbpath      flush_log    restore     set_detect_types          use
delete      help         scan        set_enforce_foreign_keys  vacuum

Unrecognized commands will be executed as queries.
Use the `query` command explicitly if you don't want to capitalize your key words.
All transactions initiated by commands are committed immediately.

chi.db>help schema
Print out the names of the database tables, their columns, and, optionally, the number of rows.
Parser help for schema:
usage: dbshell [-h] [-t [TABLES ...]] [-c]

options:
  -h, --help            show this help message and exit
  -t [TABLES ...], --tables [TABLES ...]
                        Only display info for this table(s).
  -c, --rowcount        Count and display the number of rows for each table.

chi.db>schema -c
Getting database schema...
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| Table Name           | Columns                                                                                                              | Number of Rows   |
+======================+======================================================================================================================+==================+
| business_addresses   | id, street, zip, ward, latitude, longitude                                                                           | 13276            |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| businesses           | account_number, legal_name, dba, address_id                                                                          | 14696            |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| license_codes        | code, description                                                                                                    | 146              |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| license_statuses     | id, status, description                                                                                              | 5                |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| licenses             | license_number, account_number, start_date, expiration_date, issue_date, status_id, status_change_date, license_code | 20120            |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| facility_types       | id, name                                                                                                             | 243              |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| risk_levels          | id, name                                                                                                             | 5                |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| facility_addresses   | id, street, zip, latitude, longitude, facility_type_id, risk_id                                                      | 13036            |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| inspected_businesses | id, license_number, dba, aka                                                                                         | 20120            |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| inspection_types     | id, name                                                                                                             | 16               |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| result_types         | id, description                                                                                                      | 7                |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| inspections          | id, license_number, facility_address_id, inspection_type_id, result_type_id, date                                    | 80016            |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| violation_types      | id, name                                                                                                             | 64               |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
| violations           | id, inspection_id, violation_type_id, comment                                                                        | 312471           |
+----------------------+----------------------------------------------------------------------------------------------------------------------+------------------+
chi.db>select violation_types -o "id DESC" -l 5
Querying violation_types...
Found 5 rows:
+------+----------------------------------------------------+
| id   | name                                               |
+======+====================================================+
| 64   | Miscellaneous / Public Health Orders               |
+------+----------------------------------------------------+
| 63   | Removal Of Suspension Sign                         |
+------+----------------------------------------------------+
| 62   | Compliance With Clean Indoor Air Ordinance         |
+------+----------------------------------------------------+
| 61   | Summary Report Displayed And Visible To The Public |
+------+----------------------------------------------------+
| 60   | Previous Core Violation Corrected                  |
+------+----------------------------------------------------+
5 rows from violation_types
chi.db>SELECT * FROM violation_types ORDER BY id DESC LIMIT 5;
+------+----------------------------------------------------+
| id   | name                                               |
+======+====================================================+
| 64   | Miscellaneous / Public Health Orders               |
+------+----------------------------------------------------+
| 63   | Removal Of Suspension Sign                         |
+------+----------------------------------------------------+
| 62   | Compliance With Clean Indoor Air Ordinance         |
+------+----------------------------------------------------+
| 61   | Summary Report Displayed And Visible To The Public |
+------+----------------------------------------------------+
| 60   | Previous Core Violation Corrected                  |
+------+----------------------------------------------------+
```

The `customize` command or the `custom_shell` script can be used to generate a template file in the current directory that subclasses `DBManager`.  
This allows for project specific additional commands as well as modifications of available commands.
