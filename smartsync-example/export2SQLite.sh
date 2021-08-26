#!/bin/bash
# Usage example:
# ./export2SQLite.sh jdbc:sqlite:./db1.sqlite unused unused cloned.sqlite person
SOURCE_JDBC=$1
USER=$2
PASSWORD=$3
DEST=$4
shift 4
echo Cloning to $SOURCE_DB to $DEST.
echo Table set $@
#export SQLITE_DRIVER=$HOME/.m2/repository/org/xerial/sqlite-jdbc/3.23.1/sqlite-jdbc-3.23.1.jar
export SMART_SYNC_COPY=./target/dbcopy-standalone.jar
set -x -e -u
java -cp $SMART_SYNC_COPY com.gioorgi.smartsync.DBCopy2SQLite  $SOURCE_JDBC $USER $PASSWORD $DEST $@