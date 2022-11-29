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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-db2/coltypes"
	"github.com/conduitio-labs/conduit-connector-db2/source/position"
)

const (
	trackingTablePattern = "CONDUIT_TRACKING_%s"

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
	// columnTypes column types from table.
	columnTypes map[string]string
}

// NewCombinedIterator - create new iterator.
func NewCombinedIterator(
	ctx context.Context,
	db *sqlx.DB,
	conn, table, orderingColumn string,
	cfgKeys, columns []string,
	batchSize int,
	snapshot bool,
	sdkPosition sdk.Position,
) (*CombinedIterator, error) {
	var err error

	it := &CombinedIterator{
		conn:           conn,
		table:          table,
		columns:        columns,
		orderingColumn: orderingColumn,
		batchSize:      batchSize,
		trackingTable:  fmt.Sprintf(trackingTablePattern, table),
	}

	// get column types for converting and get primary keys information
	it.columnTypes, it.keys, err = coltypes.GetColumnTypes(ctx, db, table)
	if err != nil {
		return nil, fmt.Errorf("get table column types: %w", err)
	}

	it.setKeys(cfgKeys)

	// create tracking table, create triggers for cdc logic.
	err = it.SetupCDC(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("setup cdc: %w", err)
	}

	pos, err := position.ParseSDKPosition(sdkPosition)
	if err != nil {
		return nil, fmt.Errorf("parse position: %w", err)
	}

	if snapshot && (pos == nil || pos.IteratorType == position.TypeSnapshot) {
		it.snapshot, err = newSnapshotIterator(ctx, db, it.table, orderingColumn, it.keys, columns,
			batchSize, pos, it.columnTypes)
		if err != nil {
			return nil, fmt.Errorf("new shapshot iterator: %w", err)
		}
	} else {
		it.cdc, err = newCDCIterator(ctx, db, it.table, it.trackingTable, it.keys,
			it.columns, it.batchSize, it.columnTypes, pos)
		if err != nil {
			return nil, fmt.Errorf("new shapshot iterator: %w", err)
		}
	}

	return it, nil
}

// SetupCDC - create tracking table, add columns, add triggers, set identity column.
func (c *CombinedIterator) SetupCDC(ctx context.Context, db *sqlx.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	// check if table exist.
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(queryIfExistTable, c.trackingTable))
	if err != nil {
		return fmt.Errorf("query exist table: %w", err)
	}

	defer rows.Close() //nolint:staticcheck,nolintlint

	for rows.Next() {
		var count int
		er := rows.Scan(&count)
		if er != nil {
			return fmt.Errorf("scan: %w", err)
		}

		if count == 1 {
			// table exist, setup not needed.
			return nil
		}
	}

	// create tracking table with all columns from `table`
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryCreateTable, c.trackingTable, c.table))
	if err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	// add columns to tracking table.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryAddColumns, c.trackingTable, columnOperationType,
		columnTimeCreated, columnTrackingID))
	if err != nil {
		return fmt.Errorf("add columns: %w", err)
	}

	// set not null for tracking id column.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(querySetNotNull, c.trackingTable, columnTrackingID))
	if err != nil {
		return fmt.Errorf("set not null: %w", err)
	}

	// generate identity for tracking id column.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(querySetGeneratedIdentity, c.trackingTable, columnTrackingID))
	if err != nil {
		return fmt.Errorf("generate identity: %w", err)
	}

	// reorg table.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryReorgTable, c.trackingTable))
	if err != nil {
		return fmt.Errorf("reorganize table: %w", err)
	}

	// add index.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryAddIndex, c.table, c.trackingTable, columnTrackingID))
	if err != nil {
		return fmt.Errorf("add index: %w", err)
	}

	triggersQuery := buildTriggers(c.trackingTable, c.table, c.columnTypes)

	// add trigger to catch insert.
	_, err = tx.ExecContext(ctx, triggersQuery.queryTriggerCatchInsert)
	if err != nil {
		return fmt.Errorf("add trigger catch insert: %w", err)
	}

	// add trigger to catch update.
	_, err = tx.ExecContext(ctx, triggersQuery.queryTriggerCatchUpdate)
	if err != nil {
		return fmt.Errorf("add trigger catch update: %w", err)
	}

	// add trigger to catch delete.
	_, err = tx.ExecContext(ctx, triggersQuery.queryTriggerCatchDelete)
	if err != nil {
		return fmt.Errorf("add trigger catch delete: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
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
				return false, fmt.Errorf("switch to cdc iterator: %w", err)
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

	c.cdc, err = newCDCIterator(ctx, db, c.table, c.trackingTable, c.keys,
		c.columns, c.batchSize, c.columnTypes, nil)
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
