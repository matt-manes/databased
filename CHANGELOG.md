# Changelog

## v2.4.0 (2023-05-21)

#### Refactorings

* do_query() will attempt to use griddy to display results
#### Others

* add missing tag prefix


## v2.3.0 (2023-05-13)

#### Refactorings

* use griddle.griddy in data_to_string()


## v2.2.1 (2023-05-08)

#### Fixes

* do_vacuum actually calls vacuum function now smh


## v2.2.0 (2023-05-08)

#### New Features

* add backup parser
#### Refactorings

* add timestamp option to dbshell backup command


## v2.1.0 (2023-05-08)

#### New Features

* add _disconnect decorator
* add vacuum()
* add connection_timeout property


## v2.0.1 (2023-05-07)

#### Fixes

* wrap table names in query statements in [] so things like colons don't trigger syntax errors


## v2.0.0 (2023-05-04)

#### Refactorings

* change dbmanager names to dbshell
* rename dbmanager cli script to dbshell


## v1.7.0 (2023-05-04)

#### New Features

* add add_rows()
* update supports exact_match param
* do_update prints number of updated rows
* do_query prints number of affected rows
#### Performance improvements

* make counting rows optional for do_info()
* do_add_row() prints addition success status
* add_rows() returns number of successes and number of failures
* add_row() returns whether the addition was successful
* delete() returns number of deleted rows via cursor.rowcount
#### Refactorings

* update() returns number of affected rows


## v1.6.0 (2023-05-03)

#### New Features

* add do_scan_dbs()
* add new column functionality to dbmanager
#### Refactorings

* change do_create_table to do_add_table in dbmanager for consistency
#### Docs

* update readme


## v1.5.1 (2023-05-02)

#### Fixes

* fix add_column() not updating existing rows to default value, if given
* add missing semicolons to queries
* fix data_to_string hanging in an infinite loop when current_width==terminal_width
#### Refactorings

* change do_find command to do_show
#### Docs

* update formatting and fix typos


## v1.5.0 (2023-04-27)

#### New Features

* add do_add_row()
* add functionality to add a row to database
* add do_drop_table()
* add do_create_table()
* add parser to add a table
* add do_search()
* add get_search_parser()
* add do_flush_log()
* add do_customize()
* add do_size()
* add do_delete()
* add get_update_parser()
* add do_update()
* add do_query()
* add partial_matching flag to parser
* add do_count()
* add limit arg to parser
* add order_by arg to parser
* add do_backup
#### Fixes

* correct capitalization
* fix not passing parent parsers as a list
* fix Namespace member reference
* fix _get_dict() usage
* fix error when printing default database file path
* add missing main()
* fix dbparsers import
#### Performance improvements

* add -c/--columns to search_parser
* add overwrite protection to do_customize()
* increase preloop db scanning robustness
#### Refactorings

* add root object to custom_manager template
* modify import statement
* add DataBased to imports
* alter custom manager file name formatting
* replace get_delete_parser with get_lookup_parser
* add partial_matching argument to base parser and remove get_delete_parser()
* cast default dbpath to Pathier object in case it's a string
* renamed to dbmanager.py
* rename dbname to dbpath
* delete create_manager()
* update imports
* replace dbmanager content with argshell version
* add partial matching arg to get_delete_parser()
* alter get_update_parser() argument definitions
* move parser generators and post parser functions to separate file
* do_info() takes a string argument instead of an argshell namespace
* rename do_find() to do_find_rows()
* implement usage of argshell package
#### Docs

* prune changelog
* improve type annotations
* update readme
* fix doc string
#### Others

* add imports to custom_manager.py
* add dbmanager to project.scripts
* cleanup testing lines
* remove unused import
* update ignores
* add dbshell import statement
* update ignores
* revert changing do_find() to do_find_rows()
* update doc string
* correct return type annotation


## v1.4.5 (2023-04-03)

#### Fixes

* fix condition causing infinite loop in data_to_string
#### Refactorings

* add print statements to data_to_string
* remove uneeded lambda in data_to_string


## v1.4.4 (2023-04-02)

#### Fixes

* return statement was indented one level too many


## v1.4.3 (2023-04-02)

#### Performance improvements

* rewrite data_to_string() with a different resizing algo


## v1.4.2 (2023-03-31)

#### Fixes

* fix dbmanager switching back to default dbname after user sets it with -db/--dbname switch


## v1.4.1 (2023-03-22)


## v1.4.0 (2023-03-17)

#### New Features

* set -db switch default in dbmanager to in-use dbname when it's  created
#### Fixes

* print results directly in dbmanager find() func when results are too wide for tabulator


## v1.3.0 (2023-03-13)

#### New Features

* add option to return a pandas.DataFrame from get_rows()
#### Others

* update test for get_rows
* remove duplicate dependency


## v1.2.0 (2023-03-11)

#### New Features

* add query switch

## v1.1.0 (2023-03-11)

#### New Features

* add cleanup func
* add query function
* add order_by and limit params to get_rows
* add data_to_string as DataBased staticmethod
* create parent dir of dbpath if non-existant
#### Fixes

* fix _connect setting self to args[0]
#### Performance improvements

* improve update switch
* add tests
* copy dbmanager with shutil.copyfile
#### Refactorings

* move connection decorator outside of DataBased
#### Others

* add tests


## v1.0.6 (2023-03-03)

#### Refactorings

* change return type to list
#### Others

* update readme


## v1.0.5 (2023-02-04)

#### Others

* add files