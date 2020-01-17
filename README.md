# smart-sync

A java jdbc tool to copy data between different databases. Also support dumps on SQLite/H2

SmartSync supports masking data too for GDPR compliance. 

Scrambiling is on the go.


# Usage example

Compile with:

	mvn clean compile assembly:single


# H2 copy

	java -cp './smartsync-example/target/dbcopy-standalone.jar;C:\Users\giorgig\.m2\repository\com\h2database\h2\1.4.188\h2-1.4.188.jar' com.gioorgi.smartsync.DBCopy2H2  jdbc:sqlite:./smartsync-example/db1.sqlite sa sa jdbc:h2:./dump-example.db Person


Via Eclipse launch com.nttdata.gundam.SmartSyncExample1

It will generate two sqlitedatabase (db1 and db2) and copy data from the first to the latter.

