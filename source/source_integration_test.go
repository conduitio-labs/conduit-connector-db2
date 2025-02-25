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
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-db2/source/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	queryCreateTable = `
	CREATE TABLE %s (
		id INT NOT NULL PRIMARY KEY,
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
		INSERT INTO %s VALUES 
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
		INSERT INTO %s VALUES 
		( 5, 'varchar', 'c', 'clob', 'long varchar', 'graphic', 'long vargraphic',
		 'vargraphic', 5455, 2321, 123.12, 123.1223)
	`

	queryUpdateCDCData = `
		UPDATE %s SET CL1 ='update' 
		WHERE ID = 5
	`

	queryDeleteCDCData = `
		DELETE FROM %s
	`

	queryFindTrackingTableName = `SELECT TABNAME FROM  SysCat.Tables WHERE TabName LIKE '%s_%%' LIMIT 1`
	queryDropTable             = `DROP TABLE IF EXISTS %s`
)

func TestSource_Snapshot_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfig(tableName)
	if err != nil {
		t.Skip()
	}

	err = prepareData(ctx, cfg[config.ConfigConnection], tableName)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.ConfigConnection], cfg[config.ConfigTable]) // nolint:errcheck,nolintlint

	s := NewSource()

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
	exceptedRecordPayload := opencdc.Change{
		After: opencdc.RawData(`{"CL1":"varchar","CL10":"123","CL11":123.1223,"CL2":"c","CL3":"clob","CL4":"long varchar","CL5":"graphic","CL6":"long vargraphic","CL7":"vargraphic","CL8":5455,"CL9":2321,"ID":1}`), //nolint:lll// for comparing
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
	t.Parallel()

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfig(tableName)
	if err != nil {
		t.Skip()
	}

	err = prepareData(ctx, cfg[config.ConfigConnection], tableName)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.ConfigConnection], cfg[config.ConfigTable]) // nolint:errcheck,nolintlint

	s := NewSource()

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

	var wantedKey opencdc.StructuredData
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
	t.Parallel()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfig(tableName)
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareEmptyTable(ctx, cfg[config.ConfigConnection], tableName)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.ConfigConnection], cfg[config.ConfigTable]) // nolint:errcheck,nolintlint

	s := NewSource()

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
	t.Parallel()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfig(tableName)
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareEmptyTable(ctx, cfg[config.ConfigConnection], tableName)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.ConfigConnection], cfg[config.ConfigTable]) // nolint:errcheck,nolintlint

	s := NewSource()

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
	err = prepareCDCData(ctx, cfg[config.ConfigConnection], cfg[config.ConfigTable])
	if err != nil {
		t.Fatal(err)
	}

	// Check insert.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if r.Operation != opencdc.OperationCreate {
		t.Fatal(errors.New("wrong operation"))
	}

	// Check cdc update.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if r.Operation != opencdc.OperationUpdate {
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

	if r.Operation != opencdc.OperationDelete {
		t.Fatal(errors.New("wrong operation"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_CDC_Empty_Table(t *testing.T) {
	t.Parallel()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfig(tableName)
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareEmptyTable(ctx, cfg[config.ConfigConnection], tableName)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.ConfigConnection], cfg[config.ConfigTable]) // nolint:errcheck,nolintlint

	s := NewSource()

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
	t.Parallel()

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfig(tableName)
	if err != nil {
		t.Skip()
	}

	// turn off snapshot
	cfg[config.ConfigSnapshot] = "false"

	err = prepareData(ctx, cfg[config.ConfigConnection], tableName)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.ConfigConnection], cfg[config.ConfigTable]) // nolint:errcheck,nolintlint

	s := NewSource()

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
	err = prepareCDCData(ctx, cfg[config.ConfigConnection], cfg[config.ConfigTable])
	if err != nil {
		t.Fatal(err)
	}

	// Check read. Snapshot data must be missed.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(r.Operation, opencdc.OperationCreate) {
		t.Fatal(errors.New("not wanted type"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareConfig(tableName string) (map[string]string, error) {
	connection := os.Getenv("DB2_CONNECTION")

	if connection == "" {
		return map[string]string{}, errors.New("DB2_CONNECTION env var must be set")
	}

	return map[string]string{
		config.ConfigConnection:     connection,
		config.ConfigTable:          tableName,
		config.ConfigOrderingColumn: "ID",
	}, nil
}

func prepareData(ctx context.Context, conn, tableName string) error {
	db, err := sql.Open("go_ibm_db", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTable, tableName))
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryInsertTestData, tableName))
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, conn, tableName string) error {
	db, err := sql.Open("go_ibm_db", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryDropTable, tableName))
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf(queryFindTrackingTableName, tableName))
	if err != nil {
		return fmt.Errorf("exec query find table: %w", err)
	}

	defer rows.Close() //nolint:staticcheck,nolintlint

	var name string
	for rows.Next() {
		er := rows.Scan(&name)
		if er != nil {
			return fmt.Errorf("rows scan: %w", er)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, name))
	if err != nil {
		return fmt.Errorf("exec drop table query: %w", err)
	}

	return nil
}

func prepareEmptyTable(ctx context.Context, conn, tableName string) error {
	db, err := sql.Open("go_ibm_db", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTable, tableName))
	if err != nil {
		return err
	}

	return nil
}

func prepareCDCData(ctx context.Context, conn, tableName string) error {
	db, err := sql.Open("go_ibm_db", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryInsertCDCData, tableName))
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryUpdateCDCData, tableName))
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryDeleteCDCData, tableName))
	if err != nil {
		return err
	}

	return nil
}

func randomIdentifier(t *testing.T) string {
	t.Helper()

	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}
