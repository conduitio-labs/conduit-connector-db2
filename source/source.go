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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-db2/source/config"
	"github.com/conduitio-labs/conduit-connector-db2/source/iterator"
	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/ibmdb/go_ibm_db" //nolint:revive,nolintlint
	"github.com/jmoiron/sqlx"
)

//go:generate mockgen -package mock -source interface.go -destination mock/iterator.go

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   config.Config
	iterator Iterator
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware(
		// disable schema extraction by default, because the source produces raw payload data
		sdk.SourceWithSchemaExtractionConfig{
			PayloadEnabled: lang.Ptr(false),
		},
	)...)
}

// Parameters returns a map of named config.Parameters that describe how to configure the Source.
func (s *Source) Parameters() commonsConfig.Parameters {
	return s.config.Parameters()
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw commonsConfig.Config) error {
	err := sdk.Util.ParseConfig(ctx, cfgRaw, &s.config, NewSource().Parameters())
	if err != nil {
		return err //nolint: wrapcheck // not needed here
	}

	s.config = s.config.Init()

	err = s.config.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp opencdc.Position) error {
	db, err := sqlx.Open("go_ibm_db", s.config.Connection)
	if err != nil {
		return err
	}

	s.iterator, err = iterator.NewCombinedIterator(
		ctx,
		iterator.CombinedParams{
			DB:             db,
			Conn:           s.config.Connection,
			Table:          s.config.Table,
			OrderingColumn: s.config.OrderingColumn,
			CfgKeys:        s.config.PrimaryKeys,
			Columns:        s.config.Columns,
			BatchSize:      s.config.BatchSize,
			Snapshot:       s.config.Snapshot,
			SdkPosition:    rp,
		},
	)
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}

	return nil
}

// Read gets the next object from the db2.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("source has next: %w", err)
	}

	if !hasNext {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("source next: %w", err)
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (s *Source) Ack(ctx context.Context, p opencdc.Position) error {
	return s.iterator.Ack(ctx, p)
}
