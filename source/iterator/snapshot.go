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

package iterator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-db2/coltypes"
	"github.com/conduitio-labs/conduit-connector-db2/source/position"
)

// SnapshotIterator - snapshot iterator.
type SnapshotIterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	// table - table name.
	table string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// key Name of column what iterator use for setting key in record.
	key string
	// orderingColumn Name of column what iterator using for sorting data.
	orderingColumn string
	// batchSize size of batch.
	batchSize int
	// position last recorded position.
	position *position.Position
	// columnTypes column types from table.
	columnTypes map[string]string
}

func NewSnapshotIterator(
	ctx context.Context,
	db *sqlx.DB,
	table, orderingColumn, key string,
	columns []string,
	batchSize int,
	position *position.Position,
	columnTypes map[string]string,
) (*SnapshotIterator, error) {
	var err error

	snapshotIterator := &SnapshotIterator{
		db:             db,
		table:          table,
		columns:        columns,
		key:            key,
		orderingColumn: orderingColumn,
		batchSize:      batchSize,
		position:       position,
		columnTypes:    columnTypes,
	}

	err = snapshotIterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return snapshotIterator, nil
}

// HasNext check ability to get next record.
func (i *SnapshotIterator) HasNext(ctx context.Context) (bool, error) {
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return false, nil
}

// Next get new record.
func (i *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := coltypes.TransformRow(ctx, row, i.columnTypes)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[i.orderingColumn]; !ok {
		return sdk.Record{}, ErrOrderingColumnIsNotExist
	}

	pos := position.Position{
		IteratorType:             position.TypeSnapshot,
		SnapshotLastProcessedVal: transformedRow[i.orderingColumn],
		Time:                     time.Now(),
	}

	convertedPosition, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	if _, ok := transformedRow[i.key]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	i.position = &pos

	return sdk.Record{
		Position: convertedPosition,
		Metadata: map[string]string{
			metadataTable:  i.table,
			metadataAction: string(actionInsert),
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			i.key: transformedRow[i.key],
		},
		Payload: sdk.RawData(transformedRowBytes),
	}, nil
}

// Stop shutdown iterator.
func (i *SnapshotIterator) Stop() error {
	if i.rows != nil {
		err := i.rows.Close()
		if err != nil {
			return err
		}
	}

	if i.db != nil {
		return i.db.Close()
	}

	return nil
}

// LoadRows selects a batch of rows from a database, based on the CombinedIterator's
// table, columns, orderingColumn, batchSize and the current position.
func (i *SnapshotIterator) loadRows(ctx context.Context) error {
	selectBuilder := sqlbuilder.NewSelectBuilder()

	if len(i.columns) > 0 {
		selectBuilder.Select(i.columns...)
	} else {
		selectBuilder.Select("*")
	}

	selectBuilder.From(i.table)

	if i.position != nil {
		selectBuilder.Where(
			selectBuilder.GreaterThan(i.orderingColumn, i.position.SnapshotLastProcessedVal),
		)
	}

	q, args := selectBuilder.
		OrderBy(i.orderingColumn).
		Limit(i.batchSize).
		Build()

	rows, err := i.db.QueryxContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("execute select query: %w", err)
	}

	i.rows = rows

	return nil
}
