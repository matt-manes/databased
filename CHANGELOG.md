# Changelog

## 1.1.0 (2023-03-11)

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

* build v1.0.6
* update changelog
* update readme


## v1.0.5 (2023-02-04)

#### Others

* update to build v1.0.5
* add files