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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/goleak"

	"github.com/conduitio-labs/conduit-connector-db2/config"
	s "github.com/conduitio-labs/conduit-connector-db2/source"
)

const (
	queryCreateTestTable       = `CREATE TABLE %s (id int, name VARCHAR(100))`
	queryDropTestTable         = `DROP TABLE %s`
	queryDropTestTrackingTable = `DROP TABLE IF EXISTS CONDUIT_TRACKING_%s`
	queryIfExistTable          = `SELECT count(*) AS count FROM  SysCat.Tables WHERE TabName='CONDUIT_TRACKING_%s'`
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	counter int32
}

// GenerateRecord generates a random sdk.Record.
func (d *driver) GenerateRecord(t *testing.T, operation sdk.Operation) sdk.Record {
	atomic.AddInt32(&d.counter, 1)

	return sdk.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			config.KeyTable: d.Config.DestinationConfig[config.KeyTable],
		},
		Key: sdk.StructuredData{
			"ID": d.counter,
		},
		Payload: sdk.Change{After: sdk.RawData(
			fmt.Sprintf(
				`{"ID":%d,"NAME":"%s"}`, d.counter, gofakeit.Name(),
			),
		),
		},
	}
}

//nolint:paralleltest // we don't need paralleltest for the Acceptance tests.
func TestAcceptance(t *testing.T) {
	cfg := prepareConfig(t)

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(t, cfg),
				AfterTest:         afterTest(t, cfg),
				GoleakOptions: []goleak.Option{
					// imdb library leak.
					goleak.IgnoreTopFunction("github.com/ibmdb/go_ibm_db/api._Cfunc_SQLDisconnect"),
				},
			},
		},
	})
}

// beforeTest creates new table before each test.
func beforeTest(t *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		table := randomIdentifier(t)
		t.Logf("table under test: %v", table)

		cfg[config.KeyTable] = table

		err := prepareData(t, cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func afterTest(t *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		db, err := sql.Open("go_ibm_db", cfg[config.KeyConnection])
		if err != nil {
			t.Fatal(err)
		}

		queryDropTable := fmt.Sprintf(queryDropTestTable, cfg[config.KeyTable])

		_, err = db.Exec(queryDropTable)
		if err != nil {
			t.Logf("drop test table: %v", err)
		}

		queryDropTrackingTable := fmt.Sprintf(queryDropTestTrackingTable, cfg[config.KeyTable])

		// check if table exist.
		rows, er := db.Query(fmt.Sprintf(queryIfExistTable, cfg[config.KeyTable]))
		if er != nil {
			t.Error(er)
		}

		defer rows.Close() //nolint:staticcheck,nolintlint

		for rows.Next() {
			var count int
			err = rows.Scan(&count)
			if err != nil {
				t.Error(er)
			}

			if count == 1 {
				// table exist, setup not needed.
				_, err = db.Exec(queryDropTrackingTable)
				if err != nil {
					t.Errorf("drop test tracking table: %v", err)
				}
			}
		}

		if err = db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}
}

func prepareConfig(t *testing.T) map[string]string {
	conn := os.Getenv("DB2_CONNECTION")
	if conn == "" {
		t.Skip("DB2_CONNECTION env var must be set")

		return nil
	}

	return map[string]string{
		config.KeyConnection: conn,
		s.KeyPrimaryKeys:     "ID",
		s.KeyOrderingColumn:  "ID",
	}
}

func prepareData(t *testing.T, cfg map[string]string) error {
	db, err := sql.Open("go_ibm_db", cfg[config.KeyConnection])
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTestTable, cfg[config.KeyTable]))
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
