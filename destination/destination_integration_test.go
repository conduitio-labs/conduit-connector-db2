// Copyright © 2022 Meroxa, Inc.
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

package destination

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-db2/config"
)

const (
	integrationTable = "conduit_integration_test_table"

	// queries.
	queryCreateTable = "CREATE TABLE %s (id int NOT NULL PRIMARY KEY, name varchar(20))"
	queryDropTable   = "DROP TABLE %s"
)

func TestIntegrationDestination_Write_Success(t *testing.T) {
	ctx := context.Background()

	cfg, err := prepareConfig()
	if err != nil {
		t.Skip(err)
	}

	err = prepareTable(ctx, cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[config.KeyConnection]) //nolint:errcheck,nolintlint

	dest := &Destination{}

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	count, er := dest.Write(ctx, []sdk.Record{
		{Payload: sdk.Change{After: sdk.StructuredData{
			"id":   1,
			"name": "test",
		}},
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id": "1"},
		},
		{Payload: sdk.Change{After: sdk.StructuredData{
			"id":   2,
			"name": "test2",
		}},
			Operation: sdk.OperationCreate,
			Key:       sdk.StructuredData{"id": "2"},
		},
		{Payload: sdk.Change{After: sdk.StructuredData{
			"id":   3,
			"name": "testUpdate",
		}},
			Operation: sdk.OperationUpdate,
			Key:       sdk.StructuredData{"id": "2"},
		},
		{
			Operation: sdk.OperationDelete,
			Key:       sdk.StructuredData{"id": "1"},
		},
	},
	)

	if er != nil {
		t.Error(er)
	}

	if count != 4 {
		t.Error(errors.New("count mismatch"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
	if err != nil {
		t.Error(err)
	}
}

func prepareConfig() (map[string]string, error) {
	conn := os.Getenv("DB2_CONNECTION")
	if conn == "" {
		return nil, errors.New("missed env variable 'DB2_CONNECTION'")
	}

	return map[string]string{
		config.KeyConnection: conn,
		config.KeyPrimaryKey: "id",
		config.KeyTable:      integrationTable,
	}, nil
}

func prepareTable(ctx context.Context, connection string) error {
	db, err := sql.Open("go_ibm_db", connection)
	if err != nil {
		return fmt.Errorf("connect to db2: %w", err)
	}

	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db2: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, integrationTable))
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, connection string) error {
	db, err := sql.Open("go_ibm_db", connection)
	if err != nil {
		return fmt.Errorf("connect to db2: %w", err)
	}

	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db2: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, integrationTable))
	if err != nil {
		return err
	}

	return nil
}
