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

package db2

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio-labs/conduit-connector-db2/source/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/goleak"
)

const (
	queryCreateTestTable       = `CREATE TABLE %s (id int, name VARCHAR(100))`
	queryDropTestTable         = `DROP TABLE IF EXISTS %s`
	queryFindTrackingTableName = `SELECT TABNAME FROM  SysCat.Tables WHERE TabName LIKE 'CONDUIT_%s_%%' LIMIT 1`
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	counter int32
}

// GenerateRecord generates a random opencdc.Record.
func (d *driver) GenerateRecord(_ *testing.T, operation opencdc.Operation) opencdc.Record {
	atomic.AddInt32(&d.counter, 1)

	return opencdc.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			config.ConfigTable: d.Config.DestinationConfig[config.ConfigTable],
		},
		Key: opencdc.StructuredData{
			"ID": d.counter,
		},
		Payload: opencdc.Change{After: opencdc.RawData(
			fmt.Sprintf(
				`{"ID":%d,"NAME":"%s"}`, d.counter, gofakeit.Name(),
			),
		),
		},
	}
}

//nolint:paralleltest // we don't need paralleltest for the Acceptance tests.
func TestAcceptance(t *testing.T) {
	connection := getConnection(t)

	srcConfig := map[string]string{
		config.ConfigConnection:     connection,
		config.ConfigPrimaryKeys:    "ID",
		config.ConfigOrderingColumn: "ID",
	}

	destConfig := map[string]string{
		config.ConfigConnection: connection,
	}

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      srcConfig,
				DestinationConfig: destConfig,
				BeforeTest:        beforeTest(t, srcConfig, destConfig),
				AfterTest:         afterTest(t, srcConfig),
				GoleakOptions: []goleak.Option{
					// imdb library leak.
					goleak.IgnoreTopFunction("github.com/ibmdb/go_ibm_db/api._Cfunc_SQLDisconnect"),
				},
			},
		},
	})
}

// beforeTest creates new table before each test.
func beforeTest(_ *testing.T, srcCfg map[string]string, destCfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		table := randomIdentifier(t)
		t.Logf("table under test: %v", table)

		srcCfg[config.ConfigTable] = table
		destCfg[config.ConfigTable] = table

		err := prepareData(t, srcCfg)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func afterTest(_ *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		db, err := sql.Open("go_ibm_db", cfg[config.ConfigConnection])
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(fmt.Sprintf(queryDropTestTable, cfg[config.ConfigTable]))
		if err != nil {
			t.Logf("drop test table: %v", err)
		}

		rows, err := db.Query(fmt.Sprintf(queryFindTrackingTableName, cfg[config.ConfigTable]))
		if err != nil {
			t.Errorf("find tracking table: %v", err)
		}

		defer rows.Close() //nolint:staticcheck,nolintlint

		var name string
		for rows.Next() {
			er := rows.Scan(&name)
			if er != nil {
				t.Errorf("rows scan: %v", err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Errorf("error iterating rows: %v", err)
		}

		if name != "" {
			_, err = db.Exec(fmt.Sprintf(queryDropTestTable, name))
			if err != nil {
				t.Errorf("drop test tracking table: %v", err)
			}
		}

		if err = db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}
}

func getConnection(t *testing.T) string {
	conn := os.Getenv("DB2_CONNECTION")
	if conn == "" {
		t.Skip("DB2_CONNECTION env var must be set")

		return ""
	}

	return conn
}

func prepareData(_ *testing.T, cfg map[string]string) error {
	db, err := sql.Open("go_ibm_db", cfg[config.ConfigConnection])
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTestTable, cfg[config.ConfigTable]))
	if err != nil {
		return err
	}

	db.Close()

	return nil
}

func randomIdentifier(t *testing.T) string {
	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}
