# Conduit Connector DB2

## General

The [DB2](https://www.ibm.com/db2) connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It provides both, a source and a destination DB2 connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.48.0
- (optional) [mock](https://github.com/golang/mock) 1.6.0

Connector uses [go_ibm_db](https://github.com/ibmdb/go_ibm_db) library. This library required to install clidriver


#### How to Install in Windows

```
You can install go_ibm_db with any of the following commands

go get -d github.com/ibmdb/go_ibm_db
go install github.com/ibmdb/go_ibm_db/installer@latest
go install github.com/ibmdb/go_ibm_db/installer@0.4.1

If your system already has a cli driver, add its path to your Path windows environment variable
Example: Path = C:\Program Files\IBM\IBM DATA SERVER DRIVER\bin


If your system does not have clidriver, go to the installer folder where go_ibm_db is downloaded in your system,
use the following command: 
(Example: C:\Users\uname\go\src\github.com\ibmdb\go_ibm_db\installer or C:\Users\uname\go\pkg\mod\github.com\ibmdb\go_ibm_db\installer 
 where uname is the username ) and run setup.go file (go run setup.go).


Add the path to the downloaded clidriver to the Windows Path environment variable.
(Example: Path=C:\Users\uname\go\src\github.com\ibmdb\clidriver\bin)


Script file to set environment variable 
cd .../go_ibm_db/installer
setenvwin.bat
```

#### How to Install in Linux/Mac

```
You can install go_ibm_db with any of the following commands

go get -d github.com/ibmdb/go_ibm_db
go install github.com/ibmdb/go_ibm_db/installer@latest
go install github.com/ibmdb/go_ibm_db/installer@0.4.1


If you already have clidriver on your system, set the following environment variables with path to clidriver

export DB2HOME=/home/uname/dsdriver
export CGO_CFLAGS=-I$DB2HOME/include
export CGO_LDFLAGS=-L$DB2HOME/lib 
Linux:
export LD_LIBRARY_PATH=/home/uname/dsdriver/lib
or
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$DB2HOME/lib
Mac:
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/Applications/dsdriver/lib

If you do not have clidriver on your system, use the command below
go to installer folder where go_ibm_db is downloaded on your system 
(Example: /home/uname/go/src/github.com/ibmdb/go_ibm_db/installer or /home/uname/go/pkg/mod/github.com/ibmdb/go_ibm_db/installer 
where uname is the username) and run setup.go file (go run setup.go)

Set the environment variables below with the path to the downloaded clidriver

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

For Docker Linux Container, use commands below
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
databases on DB2 for z/OS or DB2 for i(AS400) servers requires either client or server license/s.
The client side license must be copied to the license folder of your clidriver installation directory.
To activate the server license you will need to purchase DB2 Connect Unlimited for System z® and
DB2 Connect Unlimited Edition for System i®. To know more about license and purchasing cost, please contact
IBM Customer Support.

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests.

## Destination

The DB2 Destination takes a `sdk.Record` and parses it into a valid SQL query. The Destination is designed to
handle different payloads and keys. Because of this, each record is individually parsed and upserted.

### Configuration Options

| Name         | Description                                                                          | Required | Example                                                                 |
|--------------|--------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------|
| `conn`       | String line  for connection  to  DB2                                                 | **true** | HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=password |
| `table`      | The name of a table in the database that the connector should  write to, by default. | **true** | users                                                                   |
| `primaryKey` | Column name used to detect if the target table already contains the record.          | **true** | id                                                                      |

### Table name

If a record contains a `db2.table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. Thus, a Destination can support multiple tables in a single connector,
as long as the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will upsert with its current received
values. Because Keys must be unique, this can lead to overwriting and potential data loss, so the keys must be
correctly assigned from the Source.

## Source 

The DB source connects to the database using the provided connection and starts creating records for each table row 
and each detected change.

### Configuration options

| Name                    | Description                                                                                                                                                                                                   | Required | Example                                                                 |
|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------|
| `connection`            | String line  for connection  to  DB2                                                                                                                                                                          | **true** | HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=password |
| `table`                 | The name of a table in the database that the connector should  write to, by default.                                                                                                                          | **true** | users                                                                   |
| `primaryKey`            | Column name that records should use for their `Key` fields.                                                                                                                                                   | **true** | id                                                                      |
| `orderingColumn`        | The name of a column that the connector will use for ordering rows. Its values must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.                                         | **true** | id                                                                      |
| `column`                | Comma separated list of column names that should be included in each Record's payload. If the field is not empty it must contain values of the `primaryKey` and `orderingColumn` fields. By default: all rows | false    | id,name,age                                                             |
| `batchSize`             | Size of rows batch. By default is 1000                                                                                                                                                                        | false    | 100                                                                     |

### Snapshot Iterator

The snapshot iterator reads all rows from the table in batches via SELECT with fetching and ordering by `orderingColumn`.
`OrderingColumn` value must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.
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

When all records are returned, the connector switches to the CDC iterator..

### Change Data Captured (CDC)

This connector implements CDC features for DB2 by adding a tracking table and triggers to populate it. The tracking
table has the same name as a target table with the prefix `CONDUIT_TRACKING_`. The tracking table has all the
same columns as the target table plus three additional columns:

| name                            | description                                          |
|---------------------------------|------------------------------------------------------|
| `CONDUIT_TRACKING_ID`           | Autoincrement index for the position.                |
| `CONDUIT_OPERATION_TYPE`        | Operation type: `insert`, `update`, or `delete`.     |
| `CONDUIT_TRACKING_CREATED_DATE` | Date when the event was added to the tacking table.  |


Triggers have name pattern `CONDUIT_TRIGGER_{{operation_type}}_{{table}}`. 


Queries to retrieve change data from a tracking table are very similar to queries in a Snapshot iterator, but with 
`CONDUIT_TRACKING_ID` ordering column.

CDC iterator periodically clears rows which were successfully applied from tracking table. 
It collects `CONDUIT_TRACKING_ID` inside the `Ack` method into a batch and clears the tracking table every 5 seconds.

Iterator saves the last `CONDUIT_TRACKING_ID` to the position from the last successfully recorded row.

If connector stops, it will parse position from the last record and will try 
to get row where `{{CONDUIT_TRACKING_ID}}` > `{{position.CDCLastID}}`.


### CDC FAQ

#### Is it possible to add/remove/rename column to table?

Yes. You have to stop the pipeline and do the same with conduit tracking table.
For example:
```sql
ALTER TABLE CLIENTS
ADD COLUMN phone VARCHAR(18);

ALTER TABLE CONDUIT_TRACKING_CLIENTS
    ADD COLUMN phone VARCHAR(18);
```

#### I accidentally remove tracking table.

You have to restart pipeline, tracking table will be recreated by connector..

#### I accidentally remove table.

You have to stop the pipeline, remove the conduit tracking table, and then start the pipeline.

#### Is it possible to change table name?

Yes. Stop the pipeline, change the value of the `table` in the Source configuration, 
change the name of the tracking table using a pattern `CONDUIT_TRACKING_{{TABLE}}`

#### Is it possible to use two identically DB2 source  connectors with the sames configs on different pipelines ?

No. You can add more destination connectors on pipeline if you need it.

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
