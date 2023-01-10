# Conduit Connector Db2

## General

The [DB2](https://www.ibm.com/db2) connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It provides both, a source and a destination DB2 connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.48.0

The Connector uses [go_ibm_db](https://github.com/ibmdb/go_ibm_db) library. This library is required to install the 
driver to work with. See instructions how to install it on [Windows systems](https://github.com/ibmdb/go_ibm_db#how-to-install-in-windows),
[Linux/macOS systems](https://github.com/ibmdb/go_ibm_db#how-to-install-in-linuxmac). Also this connector is required enabled
CGO

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

The DB2 Destination takes a `sdk.Record` and parses it into a valid SQL query.

### Configuration Options

| Name          | Description                                                                                                                                           | Required | Example                                                                 |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------|
| `connection ` | String line for connection to DB2 ([format](https://github.com/ibmdb/go_ibm_db/blob/master/API_DOCUMENTATION.md#-1-opendrivernameconnectionstring)).  | **true** | HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=password |
| `table`       | The name of a table in the database that the connector should  write to, by default.                                                                  | **true** | users                                                                   |

### Table name

If a record contains a `db2.table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. Thus, a destination can support multiple tables in a single connector,
as long as the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will upsert with its current received
values. Because Keys must be unique, this can lead to overwriting and potential data loss, so the keys must be
correctly assigned from the Source.

## Source 

The DB source connects to the database using the provided connection and starts creating records for each table row 
and each detected change.

### Configuration options

| Name             | Description                                                                                                                                                                                                   | Required | Example                                                               |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------------------------------------------------------------------|
| `connection`     | String line for connection to DB2 ([format](https://github.com/ibmdb/go_ibm_db/blob/master/API_DOCUMENTATION.md#-1-opendrivernameconnectionstring)).                                                          | **true** | HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=password |
| `table`          | The name of a table in the database that the connector should  write to, by default.                                                                                                                          | **true** | users                                                                 |
| `orderingColumn` | The name of a column that the connector will use for ordering rows. Its values must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.                                         | **true** | id                                                                    |
| `column`         | Comma separated list of column names that should be included in each Record's payload. If the field is not empty it must contain values of the `primaryKey` and `orderingColumn` fields. By default: all rows | false    | id,name,age                                                           |
| `primaryKeys`    | Comma separated list of column names that records could use for their `Key` fields. By default connector uses primary keys from table if they are not exist connector will use ordering column.               | false    | id                                                                    |
| `snapshot`       | Whether or not the plugin will take a snapshot of the entire table before starting cdc mode, by default true.                                                                                                 | false    | false                                                                     |
| `batchSize`      | Size of rows batch. By default is 1000.                                                                                                                                                                       | false    | 100                                                                   |

### Snapshot
By default when the connector starts for the first time, snapshot mode is enabled, which means that existing data will 
be read. To skip reading existing, change config parameter `snapshot` to `false`.

First time when the snapshot iterator starts work, 
it is get max value from `orderingColumn` and saves this value to position.
The snapshot iterator reads all rows, where `orderingColumn` values less or equal maxValue, from the table in batches.


Values in the ordering column must be unique and suitable for sorting, otherwise, the snapshot won't work correctly. 
Iterators saves last processed value from `orderingColumn` column to position to field `SnapshotLastProcessedVal`. 
If snapshot was interrupted on next start connector will parse last recorded position 
to find next snapshot rows.


When all records are returned, the connector switches to the CDC iterator.

### Change Data Capture (CDC)

This connector implements CDC features for DB2 by adding a tracking table and triggers to populate it. The tracking
table has the same name as a target table with the prefix `CONDUIT_TRACKING_`. The tracking table has all the
same columns as the target table plus three additional columns:

| name                            | description                                          |
|---------------------------------|------------------------------------------------------|
| `CONDUIT_TRACKING_ID`           | Autoincrement index for the position.                |
| `CONDUIT_OPERATION_TYPE`        | Operation type: `insert`, `update`, or `delete`.     |
| `CONDUIT_TRACKING_CREATED_DATE` | Date when the event was added to the tacking table.  |

The connector saves  information about update, delete, insert `table` operations inside tracking table. 
For example if user inserts new row into `table` connector will save all new columns values inside tracking table  
with `CONDUIT_OPERATION_TYPE` = `insert`

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

#### I accidentally removed tracking table.

You have to restart pipeline, tracking table will be recreated by connector.

#### I accidentally removed table.

You have to stop the pipeline, remove the conduit tracking table, and then start the pipeline.

#### Is it possible to change table name?

Yes. Stop the pipeline, change the value of the `table` in the Source configuration, 
change the name of the tracking table using a pattern `CONDUIT_TRACKING_{{TABLE}}`

#### Is it possible to use two identical Db2 source connectors with the same configs on different pipelines?

No. You can add more destination connectors on pipeline if you need it.
