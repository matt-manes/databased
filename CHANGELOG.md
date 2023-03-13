# Changelog

## 1.3.0 (2023-03-13)

#### New Features

* add option to return a pandas.DataFrame from get_rows()
#### Others

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