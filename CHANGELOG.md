# Changelog

## 1.5.0 (2023-04-27)

#### New Features

##### dbmanager
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

#### Refactorings

* delete create_manager()
* replace dbmanager content with argshell version
* implement usage of argshell package
#### Docs

* improve type annotations
* update readme
* fix doc string
## v1.4.5 (2023-04-03)

#### Fixes

* fix condition causing infinite loop in data_to_string
#### Refactorings

* add print statements to data_to_string
* remove uneeded lambda in data_to_string
#### Others

* build v1.4.5
* update changelog


## v1.4.4 (2023-04-02)

#### Fixes

* return statement was indented one level too many
#### Others

* build v1.4.4
* update changelog


## v1.4.3 (2023-04-02)

#### Performance improvements

* rewrite data_to_string() with a different resizing algo
#### Others

* build v1.4.3
* update changelog


## v1.4.2 (2023-03-31)

#### Fixes

* fix dbmanager switching back to default dbname after user sets it with -db/--dbname switch
#### Others

* build v1.4.2
* update changelog


## v1.4.1 (2023-03-22)

#### Others

* build v1.4.1


## v1.4.0 (2023-03-17)

#### New Features

* set -db switch default in dbmanager to in-use dbname when it's  created
#### Fixes

* print results directly in dbmanager find() func when results are too wide for tabulator
#### Others

* build v1.4.0
* update changelog


## v1.3.0 (2023-03-13)

#### New Features

* add option to return a pandas.DataFrame from get_rows()
#### Others

* build v1.3.0
* update changelog
* update test for get_rows
* remove duplicate dependency


## v1.2.0 (2023-03-11)

#### New Features

* add query switch
#### Others

* build v1.2.0
* update changelog


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

* build v1.1.0
* update changelog
* add tests


## v1.0.6 (2023-03-03)

#### Refactorings

* change return type to list
#### Others

* build v1.0.6
* update changelog
* update readme


## v1.0.5 (2023-02-04)

#### Others

* update to build v1.0.5
* add files