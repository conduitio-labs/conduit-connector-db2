# Conduit Connector DB2

## General

The [DB2](https://www.ibm.com/db2) connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It provides both, a source and a destination DB2 connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.48.0
- (optional) [mock](https://github.com/golang/mock) 1.6.0

Connector uses go_ibm_db[go_ibm_db](https://github.com/ibmdb/go_ibm_db) library. This library required to install clidriver


#### How to Install in Windows

```
You may install go_ibm_db using either of below commands
go get -d github.com/ibmdb/go_ibm_db
go install github.com/ibmdb/go_ibm_db/installer@latest
go install github.com/ibmdb/go_ibm_db/installer@0.4.1

If you already have a cli driver available in your system, add the path of the same to your Path windows environment variable
Example: Path = C:\Program Files\IBM\IBM DATA SERVER DRIVER\bin


If you do not have a clidriver in your system, go to installer folder where go_ibm_db is downloaded in your system, use below command: 
(Example: C:\Users\uname\go\src\github.com\ibmdb\go_ibm_db\installer or C:\Users\uname\go\pkg\mod\github.com\ibmdb\go_ibm_db\installer 
 where uname is the username ) and run setup.go file (go run setup.go).


Add the path of the clidriver downloaded to your Path windows environment variable
(Example: Path=C:\Users\uname\go\src\github.com\ibmdb\clidriver\bin)


Script file to set environment variable 
cd .../go_ibm_db/installer
setenvwin.bat
```

#### How to Install in Linux/Mac

```
You may install go_ibm_db using either of below commands
go get -d github.com/ibmdb/go_ibm_db
go install github.com/ibmdb/go_ibm_db/installer@latest
go install github.com/ibmdb/go_ibm_db/installer@0.4.1


If you already have a cli driver available in your system, set the below environment variables with the clidriver path

export DB2HOME=/home/uname/dsdriver
export CGO_CFLAGS=-I$DB2HOME/include
export CGO_LDFLAGS=-L$DB2HOME/lib 
Linux:
export LD_LIBRARY_PATH=/home/uname/dsdriver/lib
or
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$DB2HOME/lib
Mac:
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/Applications/dsdriver/lib

If you do not have a clidriver available in your system, use below command:
go to installer folder where go_ibm_db is downloaded in your system 
(Example: /home/uname/go/src/github.com/ibmdb/go_ibm_db/installer or /home/uname/go/pkg/mod/github.com/ibmdb/go_ibm_db/installer 
where uname is the username) and run setup.go file (go run setup.go)

Set the below envronment variables with the path of the clidriver downloaded

export DB2HOME=/home/uname/go/src/github.com/ibmdb/clidriver
export CGO_CFLAGS=-I$DB2HOME/include
export CGO_LDFLAGS=-L$DB2HOME/lib
Linux:
export LD_LIBRARY_PATH=/home/uname/go/src/github.com/ibmdb/clidriver/lib
or
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$DB2HOME/lib
Mac:
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/home/uname/go/src/github.com/ibmdb/clidriver/lib
or
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$DB2HOME/lib


Script file to set environment variables in Linux/Mac 
cd .../go_ibm_db/installer
source setenv.sh

For Docker Linux Container, use below commands
yum install -y gcc git go wget tar xz make gcc-c++
cd /root
curl -OL https://golang.org/dl/go1.17.X.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.17.X.linux-amd64.tar.gz

rm /usr/bin/go
rm /usr/bin/gofmt
cp /usr/local/go/bin/go /usr/bin/
cp /usr/local/go/bin/gofmt /usr/bin/

go install github.com/ibmdb/go_ibm_db/installer@v0.4.1
or 
go install github.com/ibmdb/go_ibm_db/installer@latest

```

#### Go_ibm_db License requirements for connecting to databases
go_ibm_db driver can connect to DB2 on Linux Unix and Windows without any additional license/s, however, connecting to
databases on DB2 for z/OS or DB2 for i(AS400) Servers require either client side or server side license/s.
The client side license would need to be copied under license folder of your clidriver installation directory 
and for activating server side license, you would need to purchase DB2 Connect Unlimited for System z® 
and DB2 Connect Unlimited Edition for System i®. To know more about license and purchasing cost, please contact
IBM Customer Support.

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests, which require Docker and Compose V2 to be installed and running. 
The command will handle starting and stopping docker containers for you.

## Destination

The DB2 Destination takes a `sdk.Record` and parses it into a valid SQL query. The Destination is designed to handle different payloads and keys.
Because of this, each record is individually parsed and upserted.

### Configuration Options

| Name         | Description                                                                          | Required | Example                                                                 |
|--------------|--------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------|
| `conn`       | String line  for connection  to  DB2                                                 | **true** | HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=password |
| `table`      | The name of a table in the database that the connector should  write to, by default. | **true** | users                                                                   |
| `primaryKey` | Column name used to detect if the target table already contains the record.          | **true** | id                                                                      |

### Table name

If a record contains a `table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. This way the Destination can support multiple tables in the same
connector, provided the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will upsert with its current received
values. Because Keys must be unique, this can overwrite and thus potentially lose data, so keys should be assigned
correctly from the Source.

## Source 

The DB source connects to the database using the provided `conn` and starts creating records for each table row and
each change detected.

### Configuration options

| Name             | Description                                                                                                                                                                                                   | Required | Example                                                                 |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------|
| `conn`           | String line  for connection  to  DB2                                                                                                                                                                          | **true** | HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=password |
| `table`          | The name of a table in the database that the connector should  write to, by default.                                                                                                                          | **true** | users                                                                   |
| `primaryKey`     | Column name that records should use for their `Key` fields.                                                                                                                                                   | **true** | id                                                                      |
| `orderingColumn` | The name of a column that the connector will use for ordering rows. Its values must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.                                         | **true** | id                                                                      |
| `column`         | Comma separated list of column names that should be included in each Record's payload. If the field is not empty it must contain values of the `primaryKey` and `orderingColumn` fields. By default: all rows | false    | id,name,age                                                             |
| `batchSize`      | Size of rows batch. By default is 1000                                                                                                                                                                        | false    | 100                                                                     |

### Snapshot Iterator

The snapshot iterator reads all rows from the table in batches via SELECT with fetching and ordering by `orderingColumn`.
`OrderingColumn` value must be unique and suitable for sorting, otherwise, the snapshot won't work correctly
Iterators saves last processed value from `primaryKey` column to position to field `SnapshotLastProcessedVal`. If snapshot stops,
it will parse position from last record and will try gets row where `{{keyColumn}} > {{position.SnapshotLastProcessedVal}}`

Example of a query:

```
SELECT {{columns...}}
FROM {{table}}
ORDER BY {{orderingColumn}}
WHERE {{keyColumn}} > {{position.SnapshotLastProcessedVal}}
LIMIT {{batchSize}};
```

When all records have been returned, the connector switches to the CDC iterator.

### Change Data Captured (CDC)

This connector implements CDC features for Oracle by adding a tracking table and triggers to populate it. The tracking
table has the same name as a target table with the prefix `CONDUIT_TRACKING_`. The tracking table has all the
same columns as the target table plus three additional columns:

| name                            | description                                          |
|---------------------------------|------------------------------------------------------|
| `CONDUIT_TRACKING_ID`           | Autoincrement index for the position.                |
| `CONDUIT_OPERATION_TYPE`        | Operation type: `insert`, `update`, or `delete`.     |
| `CONDUIT_TRACKING_CREATED_DATE` | Date when the event was added to the tacking table.  |


Triggers have name pattern `CONDUIT_TRIGGER_{{operation_type}}_{{table}}`. 


The queries to get change data from the tracking table look pretty similar to queries in the Snapshot iterator, but
with `CONDUIT_TRACKING_ID` ordering column.

CDC iterator periodically clears rows which were successfully applied from tracking table. It is collects `CONDUIT_TRACKING_ID`
inside `Ack` method  to the batch and clears tracking table each 5 seconds

Iterator saves last `CONDUIT_TRACKING_ID` to position from last successfully recorded row.

If connector stops,
it will parse position from last record and will try gets row where `{{CONDUIT_TRACKING_ID}} > {{position.CDCLastID}}`


### CDC FAQ

#### Is it possible to add/remove/rename column to table?

Yes. You have to stop pipeline and do the same thing to conduit tracking table too.
For example:
```sql
ALTER TABLE CLIENTS
ADD COLUMN phone VARCHAR(18);

ALTER TABLE CONDUIT_TRACKING_CLIENTS
    ADD COLUMN phone VARCHAR(18);
```

#### I accidentally remove tracking table.

You have to restart pipeline, tracking table will be recreating by connector.

#### I accidentally remove table.

You have stop pipeline, remove conduit tracking table, then start pipeline.

#### Is it possible to change table name?

Yes. Please stop pipeline, change `table` value in source config, please change tracking table name uses pattern
`CONDUIT_TRACKING_{{TABLE}}`


### Position

Position looks like:

```go
type Position struct {
	// IteratorType - shows in what iterator was created position.
	IteratorType IteratorType

	// Snapshot information.
	// SnapshotLastProcessedVal - last processed value from ordering column.
	SnapshotLastProcessedVal any

	// CDC information.
	// CDCID - last processed id from tracking table.
	CDCLastID int

	// Time Created time.
	Time time.Time
}
```

Example of position:

```json
{
  "iteratorType": "s",
  "snapshotLastProcessedVal": 16,
  "cdcLastID" : 3,
  "time":"2021-02-18T21:54:42.123Z" 
}
```
