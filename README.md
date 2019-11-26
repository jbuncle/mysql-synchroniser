# MySQL Synchroniser

[![Build Status](https://travis-ci.org/jbuncle/mysql-synchroniser.svg?branch=master)](https://travis-ci.org/jbuncle/mysql-synchroniser)
[![codecov.io](https://codecov.io/github/jbuncle/mysql-synchroniser/coverage.svg?branch=master)](https://codecov.io/github/jbuncle/mysql-synchroniser?branch=master)
[![codacy.com](https://api.codacy.com/project/badge/15f4db01f834486b82e9a704f3ea2b28)](https://www.codacy.com/public/jbuncle/mysql-synchroniser.git)

Java library for Synchronising MySQL Schema Structures. 

Provides the ability to re-synchronise 2 MySQL instances by applying differences in the table & database structure (essentially it generates a MySQL Diff file). 

The generated MySQL script can be used to apply to the target table based on the structural differences to a source table, making it possible to automatically create scripts at build time for publishing database changes.

This is useful for Continuous Integration or DevOps systems where you want to 'release' database changes in an automated fashion.

This could easily be a Command Line Application, however I've left it as a library to allow it to be integrated into different systems more easily be maximising flexibility.

## Basic Usage

```java
// Create datasource for target database (the database structure we want to update)
final MysqlDataSource target = new MysqlDataSource();
target.setUser("root");
target.setServerName("localhost");
target.setDatabaseName("target");

// Create datasource for source database (with the database structure we want to update to)
final MysqlDataSource source = new MysqlDataSource();
source.setUser("root");
source.setServerName("localhost");
source.setDatabaseName("source");

try {
    // Generate List of MySQL statements to update 
    final List<String> list = ScriptGenerator.compareSchema(source, target);
    //Print MySQL update statements
    for (String update : list) {
        System.out.println(update);
    }
} catch (SQLException ex) {
    Logger.getLogger(Example.class.getName()).log(Level.SEVERE, null, ex);
}
```


## Installation

This library can be installed using JitPack (which supports Maven & Gradle)
https://jitpack.io/#jbuncle/mysql-synchroniser, as it isn't in standard repositories.
