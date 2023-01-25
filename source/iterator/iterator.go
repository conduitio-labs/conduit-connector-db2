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

package iterator

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-db2/coltypes"
	"github.com/conduitio-labs/conduit-connector-db2/source/position"
)

const (
	trackingTablePattern = "CONDUIT_%s_%s"

	// tracking table columns.
	columnOperationType = "CONDUIT_OPERATION_TYPE"
	columnTimeCreated   = "CONDUIT_TRACKING_CREATED_DATE"
	columnTrackingID    = "CONDUIT_TRACKING_ID"
)

// CombinedIterator combined iterator.
type CombinedIterator struct {
	cdc      *cdcIterator
	snapshot *snapshotIterator

	// connection string.
	conn string

	// table - table name.
	table string
	// trackingTable - tracking table name.
	trackingTable string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// keys Names of columns what iterator use for setting key in record.
	keys []string
	// orderingColumn Name of column what iterator use for sorting data.
	orderingColumn string
	// batchSize size of batch.
	batchSize int
	// info about table
	tableInfo coltypes.TableInfo
}

// CombinedParams is an incoming params for the [NewCombinedIterator] function.
type CombinedParams struct {
	DB             *sqlx.DB
	Conn           string
	Table          string
	OrderingColumn string
	CfgKeys        []string
	Columns        []string
	BatchSize      int
	Snapshot       bool
	SdkPosition    sdk.Position
}

// NewCombinedIterator - create new iterator.
func NewCombinedIterator(ctx context.Context, params CombinedParams) (*CombinedIterator, error) {
	var err error

	pos, err := position.ParseSDKPosition(params.SdkPosition)
	if err != nil {
		return nil, fmt.Errorf("parse position: %w", err)
	}

	suffixName := getSuffixName(pos)

	it := &CombinedIterator{
		conn:           params.Conn,
		table:          params.Table,
		columns:        params.Columns,
		orderingColumn: params.OrderingColumn,
		batchSize:      params.BatchSize,
		trackingTable:  fmt.Sprintf(trackingTablePattern, params.Table, suffixName),
	}

	// get column types for converting and get primary keys information
	it.tableInfo, err = coltypes.GetTableInfo(ctx, params.DB, params.Table)
	if err != nil {
		return nil, fmt.Errorf("get table info: %w", err)
	}

	it.setKeys(params.CfgKeys)

	// create tracking table, create triggers for cdc logic.
	err = setupCDC(ctx, params.DB, it.table, it.trackingTable, suffixName, it.tableInfo)
	if err != nil {
		return nil, fmt.Errorf("setup cdc: %w", err)
	}

	if params.Snapshot && (pos == nil || pos.IteratorType == position.TypeSnapshot) {
		it.snapshot, err = newSnapshotIterator(ctx, snapshotParams{
			db:             params.DB,
			table:          params.Table,
			orderingColumn: params.OrderingColumn,
			keys:           it.keys,
			columns:        params.Columns,
			batchSize:      params.BatchSize,
			position:       pos,
			columnTypes:    it.tableInfo.ColumnTypes,
			suffixName:     suffixName,
		})
		if err != nil {
			return nil, fmt.Errorf("new shapshot iterator: %w", err)
		}
	} else {
		it.cdc, err = newCDCIterator(ctx, cdcParams{
			db:            params.DB,
			table:         it.table,
			trackingTable: it.trackingTable,
			keys:          it.keys,
			columns:       it.columns,
			batchSize:     it.batchSize,
			columnTypes:   it.tableInfo.ColumnTypes,
			position:      pos,
		})
		if err != nil {
			return nil, fmt.Errorf("new shapshot iterator: %w", err)
		}
	}

	return it, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
// If the underlying snapshot iterator returns false, the combined iterator will try to switch to the cdc iterator.
func (c *CombinedIterator) HasNext(ctx context.Context) (bool, error) {
	switch {
	case c.snapshot != nil:
		hasNext, err := c.snapshot.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot has next: %w", err)
		}

		if !hasNext {
			if er := c.switchToCDCIterator(ctx); er != nil {
				return false, fmt.Errorf("switch to cdc iterator: %w", er)
			}

			return false, nil
		}

		return true, nil

	case c.cdc != nil:
		return c.cdc.HasNext(ctx)

	default:
		return false, nil
	}
}

// Next returns the next record.
func (c *CombinedIterator) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case c.snapshot != nil:
		return c.snapshot.Next(ctx)

	case c.cdc != nil:
		return c.cdc.Next(ctx)

	default:
		return sdk.Record{}, ErrNoInitializedIterator
	}
}

// Stop the underlying iterators.
func (c *CombinedIterator) Stop() error {
	if c.snapshot != nil {
		return c.snapshot.Stop()
	}

	if c.cdc != nil {
		return c.cdc.Stop()
	}

	return nil
}

// Ack check if record with position was recorded.
func (c *CombinedIterator) Ack(ctx context.Context, rp sdk.Position) error {
	pos, err := position.ParseSDKPosition(rp)
	if err != nil {
		return fmt.Errorf("parse position: %w", err)
	}

	if pos.IteratorType == position.TypeCDC {
		return c.cdc.Ack(ctx, pos)
	}

	return nil
}

func (c *CombinedIterator) switchToCDCIterator(ctx context.Context) error {
	var err error

	err = c.snapshot.Stop()
	if err != nil {
		return fmt.Errorf("stop snaphot iterator: %w", err)
	}

	c.snapshot = nil

	db, err := sqlx.Open("go_ibm_db", c.conn)
	if err != nil {
		return err
	}

	c.cdc, err = newCDCIterator(ctx, cdcParams{
		db:            db,
		table:         c.table,
		trackingTable: c.trackingTable,
		keys:          c.keys,
		columns:       c.columns,
		batchSize:     c.batchSize,
		columnTypes:   c.tableInfo.ColumnTypes,
		position:      nil,
	})
	if err != nil {
		return fmt.Errorf("new cdc iterator: %w", err)
	}

	return nil
}

func (c *CombinedIterator) setKeys(cfgKeys []string) {
	// first priority keys from config.
	if len(cfgKeys) > 0 {
		c.keys = cfgKeys

		return
	}

	// second priority primary keys from table.
	if len(c.keys) > 0 {
		return
	}

	// last priority ordering column.
	c.keys = []string{c.orderingColumn}
}

func getSuffixName(pos *position.Position) string {
	// get suffix from position
	if pos != nil {
		return pos.SuffixName
	}

	// create new suffix
	return time.Now().Format("150405")
}
