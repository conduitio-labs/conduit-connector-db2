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

The DB2 Destination takes a `sdk.Record` and parses it into a valid SQL query.

### Configuration Options

| Name               | Description                                                                          | Required | Example                                                                 |
|--------------------|--------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------|
| `connection`       | String line  for connection  to  DB2                                                 | **true** | HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=password |
| `table`            | The name of a table in the database that the connector should  write to, by default. | **true** | users                                                                   |
| `primaryKey`       | Column name used to detect if the target table already contains the record.          | **true** | id                                                                      |

### Table name

If a record contains a `db2.table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. Thus, a Destination can support multiple tables in a single connector,
as long as the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will upsert with its current received
values. Because Keys must be unique, this can lead to overwriting and potential data loss, so the keys must be
correctly assigned from the Source.
