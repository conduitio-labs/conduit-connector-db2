// Copyright © 2022 Meroxa, Inc & Yalantis.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-db2/config"
)

const (
	table            = "CONDUIT_SOURCE_INTEGRATION_TABLE"
	queryCreateTable = `
	CREATE TABLE CONDUIT_SOURCE_INTEGRATION_TABLE (
		id int NOT NULL PRIMARY KEY,
		cl1 VARCHAR(15),
		cl2 CHAR,
		cl3 CLOB,
		cl4 LONG VARCHAR,
		cl5 GRAPHIC(7),
		cl6 LONG VARGRAPHIC,
		cl7 VARGRAPHIC(15),
		cl8 BIGINT,
		cl9 SMALLINT,
		cl10 DECIMAL,
		cl11 FLOAT
)
	`
	queryInsertTestData = `
		INSERT INTO CONDUIT_SOURCE_INTEGRATION_TABLE VALUES 
		( 1, 'varchar', 'c', 'clob', 'long varchar', 'graphic', 'long vargraphic',
		 'vargraphic', 5455, 2321, 123.12, 123.1223),
		( 2, 'varchar', 'c', 'clob', 'long varchar', 'graphic', 'long vargraphic', 
		 'vargraphic', 5455, 2321, 123.12, 123.1223),
		( 3, 'varchar', 'c', 'clob', 'long varchar', 'graphic', 'long vargraphic',
		 'vargraphic', 5455, 2321, 123.12, 123.1223),
		( 4, 'varchar', 'c', 'clob', 'long varchar', 'graphic', 'long vargraphic', 
		 'vargraphic', 5455, 2321, 123.12, 123.1223)
`
	queryInsertCDCData = `
		INSERT INTO CONDUIT_SOURCE_INTEGRATION_TABLE VALUES 
		( 5, 'varchar', 'c', 'clob', 'long varchar', 'graphic', 'long vargraphic',
		 'vargraphic', 5455, 2321, 123.12, 123.1223)
	`

	queryUpdateCDCData = `
		UPDATE CONDUIT_SOURCE_INTEGRATION_TABLE SET CL1 ='update' 
		WHERE ID = 5
	`

	queryDeleteCDCData = `
		DELETE FROM CONDUIT_SOURCE_INTEGRATION_TABLE
	`

	queryDropTable         = `DROP TABLE CONDUIT_SOURCE_INTEGRATION_TABLE`
	queryDropTrackingTable = `DROP TABLE CONDUIT_TRACKING_CONDUIT_SOURCE_INTEGRATION_TABLE`
)

func TestSource_Snapshot_Success(t *testing.T) {
	ctx := context.Background()

	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	err = prepareData(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.KeyConnection]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check first read.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// check right converting.
	exceptedRecordPayload := sdk.Change{
		After: sdk.RawData(`{"CL1":"varchar","CL10":"123","CL11":123.1223,"CL2":"c","CL3":"clob","CL4":"long varchar","CL5":"graphic","CL6":"long vargraphic","CL7":"vargraphic","CL8":5455,"CL9":2321,"ID":1}`), //nolint:lll// for comparing
	}

	if !reflect.DeepEqual(r.Payload, exceptedRecordPayload) {
		t.Fatal(errors.New("wrong record payload"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Continue(t *testing.T) {
	ctx := context.Background()

	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	err = prepareData(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.KeyConnection]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check first read.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var wantedKey sdk.StructuredData
	wantedKey = map[string]interface{}{"ID": int32(1)}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Fatal(errors.New("wrong record key"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Open from previous position.
	err = s.Open(ctx, r.Position)
	if err != nil {
		t.Fatal(err)
	}

	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedKey = map[string]interface{}{"ID": int32(2)}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Fatal(errors.New("wrong record key"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Empty_Table(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareEmptyTable(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.KeyConnection]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check read from empty table.
	_, err = s.Read(ctx)
	if err != sdk.ErrBackoffRetry {
		t.Fatal(err)
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_CDC(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareEmptyTable(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.KeyConnection]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check read from empty table.
	_, err = s.Read(ctx)
	if err != sdk.ErrBackoffRetry {
		t.Fatal(err)
	}

	// load data for cdc.
	err = prepareCDCData(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	// Check insert.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if r.Operation != sdk.OperationCreate {
		t.Fatal(errors.New("wrong operation"))
	}

	// Check cdc update.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if r.Operation != sdk.OperationUpdate {
		t.Fatal(errors.New("wrong operation"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// start with previous position.
	err = s.Open(ctx, r.Position)
	if err != nil {
		t.Fatal(err)
	}

	// Check cdc delete.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if r.Operation != sdk.OperationDelete {
		t.Fatal(errors.New("wrong operation"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_CDC_Empty_Table(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareEmptyTable(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.KeyConnection]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check read from empty table.
	_, err = s.Read(ctx)
	if err != sdk.ErrBackoffRetry {
		t.Fatal(err)
	}

	// CDC iterator read from empty table.
	_, err = s.Read(ctx)
	if err != sdk.ErrBackoffRetry {
		t.Fatal(err)
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Off(t *testing.T) {
	ctx := context.Background()

	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	// turn off snapshot
	cfg[KeySnapshot] = "false"

	err = prepareData(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.KeyConnection]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// load data for cdc.
	err = prepareCDCData(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	// Check read. Snapshot data must be missed.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(r.Operation, sdk.OperationCreate) {
		t.Fatal(errors.New("not wanted type"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareConfig() (map[string]string, error) {
	connection := os.Getenv("DB2_CONNECTION")

	if connection == "" {
		return map[string]string{}, errors.New("DB2_CONNECTION env var must be set")
	}

	return map[string]string{
		config.KeyConnection: connection,
		config.KeyTable:      table,
		KeyOrderingColumn:    "ID",
	}, nil
}

func prepareData(ctx context.Context, conn string) error {
	db, err := sql.Open("go_ibm_db", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(queryCreateTable)
	if err != nil {
		return err
	}

	_, err = db.Exec(queryInsertTestData)
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, conn string) error {
	db, err := sql.Open("go_ibm_db", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(queryDropTable)
	if err != nil {
		return err
	}

	_, err = db.Exec(queryDropTrackingTable)
	if err != nil {
		return err
	}

	return nil
}

func prepareEmptyTable(ctx context.Context, conn string) error {
	db, err := sql.Open("go_ibm_db", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(queryCreateTable)
	if err != nil {
		return err
	}

	return nil
}

func prepareCDCData(ctx context.Context, conn string) error {
	db, err := sql.Open("go_ibm_db", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(queryInsertCDCData)
	if err != nil {
		return err
	}

	_, err = db.Exec(queryUpdateCDCData)
	if err != nil {
		return err
	}

	_, err = db.Exec(queryDeleteCDCData)
	if err != nil {
		return err
	}

	return nil
}
