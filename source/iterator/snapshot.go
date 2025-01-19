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
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-db2/coltypes"
	"github.com/conduitio-labs/conduit-connector-db2/source/position"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

// snapshotIterator - iterator which get snapshot data.
// A "snapshot" is the state of a table data at a particular point in time when connector starts work.
// The first time when the snapshot iterator starts work, it is gets max value from `orderingColumn` and saves
// this value to position.
// The snapshot iterator reads all rows, where `orderingColumn` values less or equal maxValue,
// from the table in batches.
// Values in the ordering column must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.
// Iterators saves last processed value from `orderingColumn` column to position to field `SnapshotLastProcessedVal`.
// If snapshot stops it will parse position from last record and will
// try gets row where `{{orderingColumn}} > {{position.SnapshotLastProcessedVal}}`.
type snapshotIterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	// table - table name.
	table string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// keys Names of columns what iterator use for setting key in record.
	keys []string
	// orderingColumn Name of column what iterator using for sorting data.
	orderingColumn string
	// maxValue max value from ordering column. Connector uses this variable like boundary value for snapshot.
	maxValue any
	// batchSize size of batch.
	batchSize int
	// position last recorded position.
	position *position.Position
	// columnTypes column types from table.
	columnTypes map[string]string
	// suffixName special suffix that connector uses for identify tracking table and triggers.
	suffixName string
}

type snapshotParams struct {
	db             *sqlx.DB
	table          string
	orderingColumn string
	keys           []string
	columns        []string
	batchSize      int
	position       *position.Position
	columnTypes    map[string]string
	suffixName     string
}

func newSnapshotIterator(
	ctx context.Context,
	params snapshotParams,
) (*snapshotIterator, error) {
	var err error

	it := &snapshotIterator{
		db:             params.db,
		table:          params.table,
		columns:        params.columns,
		keys:           params.keys,
		orderingColumn: params.orderingColumn,
		batchSize:      params.batchSize,
		position:       params.position,
		columnTypes:    params.columnTypes,
		suffixName:     params.suffixName,
	}

	err = it.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	if params.position != nil {
		it.maxValue = params.position.SnapshotMaxValue
	} else {
		err = it.setMaxValue(ctx)
		if err != nil {
			return nil, fmt.Errorf("set max value: %w", err)
		}
	}

	return it, nil
}

// HasNext check ability to get next record.
func (i *snapshotIterator) HasNext(ctx context.Context) (bool, error) {
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	// check new batch.
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	return false, nil
}

// Next get new record.
func (i *snapshotIterator) Next(ctx context.Context) (opencdc.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return opencdc.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := coltypes.TransformRow(ctx, row, i.columnTypes)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[i.orderingColumn]; !ok {
		return opencdc.Record{}, ErrNoOrderingColumn
	}

	pos := position.Position{
		IteratorType:             position.TypeSnapshot,
		SnapshotLastProcessedVal: transformedRow[i.orderingColumn],
		SnapshotMaxValue:         i.maxValue,
		SuffixName:               i.suffixName,
	}

	sdkPos, err := pos.ConvertToSDKPosition()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("convert position %w", err)
	}

	keysMap := make(map[string]any)
	for _, val := range i.keys {
		if _, ok := transformedRow[val]; !ok {
			return opencdc.Record{}, fmt.Errorf("key %v, %w", val, ErrNoKey)
		}

		keysMap[val] = transformedRow[val]
	}

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	i.position = &pos

	metadata := opencdc.Metadata(map[string]string{metadataTable: i.table})
	metadata.SetCreatedAt(time.Now())

	return sdk.Util.Source.NewRecordSnapshot(
			sdkPos,
			metadata,
			opencdc.StructuredData(keysMap),
			opencdc.RawData(transformedRowBytes)),
		nil
}

// Stop shutdown iterator.
func (i *snapshotIterator) Stop() error {
	if i.rows != nil {
		err := i.rows.Close()
		if err != nil {
			return fmt.Errorf("close rows")
		}
	}

	if i.db != nil {
		return i.db.Close()
	}

	return nil
}

// LoadRows selects a batch of rows from a database, based on the CombinedIterator's
// table, columns, orderingColumn, batchSize and the current position.
func (i *snapshotIterator) loadRows(ctx context.Context) error {
	builder := sqlbuilder.NewSelectBuilder()

	if len(i.columns) > 0 {
		builder.Select(i.columns...)
	} else {
		builder.Select("*")
	}

	builder.From(i.table)

	if i.position != nil {
		builder.Where(
			builder.GreaterThan(i.orderingColumn, i.position.SnapshotLastProcessedVal),
			builder.LessEqualThan(i.orderingColumn, i.position.SnapshotMaxValue),
		)
	}

	q, args := builder.
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

// getMaxValue get max value from ordered column.
func (i *snapshotIterator) setMaxValue(ctx context.Context) error {
	//nolint:sqlclosecheck // false positive https://github.com/ryanrolds/sqlclosecheck/issues/35
	rows, err := i.db.QueryxContext(ctx, fmt.Sprintf(queryGetMaxValue, i.orderingColumn, i.table))
	if err != nil {
		return fmt.Errorf("execute query get max value: %w", err)
	}
	defer rows.Close()

	var maxValue any
	for rows.Next() {
		err = rows.Scan(&maxValue)
		if err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
	}

	i.maxValue = maxValue

	return nil
}
