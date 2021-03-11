#!/bin/bash

export SQLITE_DRIVER=$HOME/.m2/repository/org/xerial/sqlite-jdbc/3.23.1/sqlite-jdbc-3.23.1.jar
export SMART_SYNC_COPY=./target/dbcopy-standalone.jar
set -x -e -u
$JAVA_HOME/bin/java -cp $SQLITE_DRIVER -cp $SMART_SYNC_COPY com.gioorgi.smartsync.DBCopy2SQLite  jdbc:sqlite:./db1.sqlite unused unused  person2