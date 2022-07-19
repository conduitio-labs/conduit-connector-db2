# Conduit Connector DB2

## General

The [DB2](https://www.ibm.com/db2) connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and a destination DB2 connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2
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

The DB2 Destination takes a `record.Record` and parses it into a valid SQL query. The Destination is designed to handle different payloads and keys.
Because of this, each record is individually parsed and upserted.

### Table name

If a record contains a `table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. This way the Destination can support multiple tables in the same
connector, provided the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will upsert with its current received
values. Because Keys must be unique, this can overwrite and thus potentially lose data, so keys should be assigned
correctly from the Source.

## Configuration Options

| name         | description                                                                                       | required |
|--------------|---------------------------------------------------------------------------------------------------| -------- |
| `conn`       | String line  for connection  to  DB2                                                              | **true** |
| `table`      | The name of a table in the database that the connector should  write to, by default.              | **true** |
| `primaryKey` | Column name used to detect if the target table already contains the record.                       | **true** |
