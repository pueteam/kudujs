# KuduJS

KuduJS is a NodeJS addon that uses the Apache Kudu C++ API to access the [Apache Kudu][kudu_home] column storage manager, which allows to use it from Javascript.

## Motivation

Kudu is a columnar storage manager developed for the Apache Hadoop platform. Kudu shares the common technical properties of Hadoop ecosystem applications: it runs on commodity hardware, is horizontally scalable, and supports highly available operation.

The default Apache Kudu implementation provides C++, Java and Python client APIs. **KuduJS** adds the NodeJS client implementation.

## Features

* Table creation
* Insert single row
* Insert multiple rows in a single call
* Update and Upsert operations
* Scan operations with predicates
* Table deletion
* (ToDo) Alter table schema

## Installation

You must have the Apache Kudu headers already installed to be able to build kuduJS.

```bash
npm install kudujs
```

## Example of use

For now please take a look at the `test.js` file.

## API

Work in progress

## License

This addon is issued under the [BSD-3-Clause](./LICENSE) license.

## Contributors

* Sergio Rodriguez de Guzman

[kudu_home]: https://kudu.apache.org
