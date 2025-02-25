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

package destination

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-db2/common"
	"github.com/conduitio-labs/conduit-connector-db2/destination/writer"
	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/ibmdb/go_ibm_db" //nolint:revive,nolintlint
)

//go:generate mockgen -package mock -source interface.go -destination mock/destination.go

// Destination DB2 Connector persists records to a db2 database.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	config common.Configuration
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named config.Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() commonsConfig.Parameters {
	return d.config.Parameters()
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg commonsConfig.Config) error {
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		return err //nolint: wrapcheck // not needed here
	}

	d.config = d.config.Init()

	err = d.config.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	db, err := sql.Open("go_ibm_db", d.config.Connection)
	if err != nil {
		return fmt.Errorf("connect to db2: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db2: %w", err)
	}

	d.writer, err = writer.NewWriter(ctx, writer.Params{
		DB:    db,
		Table: d.config.Table,
	})

	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		err := sdk.Util.Destination.Route(ctx, record,
			d.writer.Insert,
			d.writer.Update,
			d.writer.Delete,
			d.writer.Insert,
		)
		if err != nil {
			return i, fmt.Errorf("route %s: %w", record.Operation.String(), err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		return d.writer.Close(ctx)
	}

	return nil
}
