# MySQL Synchroniser

[![Build Status](https://travis-ci.org/jbuncle/mysql-synchroniser.svg?branch=master)](https://travis-ci.org/jbuncle/mysql-synchroniser)
[![Coverage Status](https://coveralls.io/repos/github/jbuncle/mysql-synchroniser/badge.svg?branch=master)](https://coveralls.io/github/jbuncle/mysql-synchroniser?branch=master)

Java library for Synchronising MySQL Schema Structures. 

Provides the ability to re-synchronise MySQL table and database structure. 

It can generate a MySQL script to update the target table based on the structural differences to a source table, making it possible to automatically create scripts at build time for publishing database changes

## Basic Usage

```java
//Compare entire database
final Connection source = ...;
final Connection target = ...;
final List&lt;String&gt; list = ScriptGenerator.compareSchema(source, target);
//Print update script
for (String update : list) {
    System.out.println(update);
}
```
