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
	"strings"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-db2/coltypes"
	"github.com/conduitio-labs/conduit-connector-db2/source/position"
)

type trackingTableService struct {
	m sync.Mutex

	// channel for getting stop signal.
	stopCh chan bool
	// channel for errors.
	errCh chan error
	// chan for notify that all queries finished and db can be closed.
	canCloseCh chan bool
	// idsForRemoving - ids of rows what need to clear.
	idsForRemoving []any
}

func newTrackingTableService() *trackingTableService {
	stopCh := make(chan bool, 1)
	canCloseCh := make(chan bool, 1)
	errCh := make(chan error, 1)
	trackingIDsForRemoving := make([]any, 0)

	return &trackingTableService{
		stopCh:         stopCh,
		errCh:          errCh,
		idsForRemoving: trackingIDsForRemoving,
		canCloseCh:     canCloseCh,
	}
}

// CDCIterator - cdc iterator.
type CDCIterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	// tableSrv service for clearing tracking table.
	tableSrv *trackingTableService

	// table - table name.
	table string
	// trackingTable - tracking table name.
	trackingTable string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// key Name of column what iterator use for setting key in record.
	key string
	// batchSize size of batch.
	batchSize int
	// position last recorded position.
	position *position.Position
	// columnTypes column types from table.
	columnTypes map[string]string
}

// NewCDCIterator create new cdc iterator.
func NewCDCIterator(
	ctx context.Context,
	db *sqlx.DB,
	table string, trackingTable, key string,
	columns []string,
	batchSize int,
	position *position.Position,
) (*CDCIterator, error) {
	var (
		err error
	)

	cdcIterator := &CDCIterator{
		db:            db,
		table:         table,
		trackingTable: trackingTable,
		columns:       columns,
		key:           key,
		batchSize:     batchSize,
		position:      position,
		tableSrv:      newTrackingTableService(),
	}

	if err = cdcIterator.loadRows(ctx); err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	// get column types for converting.
	cdcIterator.columnTypes, err = coltypes.GetColumnTypes(ctx, db, trackingTable)
	if err != nil {
		return nil, fmt.Errorf("get table column types: %w", err)
	}

	// run clearing tracking table.
	go cdcIterator.clearTrackingTable(ctx)

	return cdcIterator, nil
}

// HasNext check ability to get next record.
func (i *CDCIterator) HasNext(ctx context.Context) (bool, error) {
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return false, nil
}

// Next get new record.
func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := coltypes.TransformRow(ctx, row, i.columnTypes)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	id, ok := transformedRow[columnTrackingID].(int32)
	if !ok {
		return sdk.Record{}, ErrWrongTrackingIDType
	}

	operationType, ok := transformedRow[columnOperationType].(string)
	if !ok {
		return sdk.Record{}, ErrWrongTrackingIDType
	}

	pos := position.Position{
		IteratorType: position.TypeCDC,
		CDCLastID:    int(id),
		Time:         time.Now(),
	}

	convertedPosition, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	if _, ok = transformedRow[i.key]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	// delete tracking columns
	delete(transformedRow, columnOperationType)
	delete(transformedRow, columnTrackingID)
	delete(transformedRow, columnTimeCreated)

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	i.position = &pos

	return sdk.Record{
		Position: convertedPosition,
		Metadata: map[string]string{
			metadataTable:  i.table,
			metadataAction: strings.ToLower(operationType),
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			i.key: transformedRow[i.key],
		},
		Payload: sdk.RawData(transformedRowBytes),
	}, nil
}

// Stop shutdown iterator.
func (i *CDCIterator) Stop() error {
	// send signal for finish clear tracking table.
	i.tableSrv.stopCh <- true

	if i.rows != nil {
		err := i.rows.Close()
		if err != nil {
			return err
		}
	}

	select {
	// wait until clearing tracking table will be finished.
	case <-i.tableSrv.canCloseCh:
		if i.db != nil {
			return i.db.Close()
		}
	// waiting timeout.
	case <-time.After(waitingTimeoutSec * time.Second):
		if i.db != nil {
			return i.db.Close()
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (i *CDCIterator) Ack(ctx context.Context, pos *position.Position) error {
	if len(i.tableSrv.errCh) > 0 {
		for v := range i.tableSrv.errCh {
			return fmt.Errorf("clear tracking table: %w", v)
		}
	}

	i.tableSrv.m.Lock()

	if i.tableSrv.idsForRemoving == nil {
		i.tableSrv.idsForRemoving = make([]any, 0)
	}

	i.tableSrv.idsForRemoving = append(i.tableSrv.idsForRemoving, pos.CDCLastID)

	i.tableSrv.m.Unlock()

	return nil
}

// LoadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (i *CDCIterator) loadRows(ctx context.Context) error {
	selectBuilder := sqlbuilder.NewSelectBuilder()

	if len(i.columns) > 0 {
		// append additional columns
		selectBuilder.Select(append(i.columns,
			[]string{columnTrackingID, columnOperationType, columnTimeCreated}...)...)
	} else {
		selectBuilder.Select("*")
	}

	selectBuilder.From(i.trackingTable)

	if i.position != nil {
		selectBuilder.Where(
			selectBuilder.GreaterThan(columnTrackingID, i.position.CDCLastID),
		)
	}

	q, args := selectBuilder.
		OrderBy(columnTrackingID).
		Limit(i.batchSize).
		Build()

	rows, err := i.db.QueryxContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("execute select query: %w", err)
	}

	i.rows = rows

	return nil
}

// deleteRows - delete rows from tracking table.
func (i *CDCIterator) deleteRows(ctx context.Context) error {
	i.tableSrv.m.Lock()
	defer i.tableSrv.m.Unlock()

	if len(i.tableSrv.idsForRemoving) == 0 {
		return nil
	}

	tx, err := i.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	deleteBuilder := sqlbuilder.NewDeleteBuilder()

	q, args := deleteBuilder.
		DeleteFrom(i.trackingTable).
		Where(deleteBuilder.In(columnTrackingID, i.tableSrv.idsForRemoving...)).
		Build()

	_, err = tx.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("execute delete query: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	i.tableSrv.idsForRemoving = nil

	return nil
}

func (i *CDCIterator) clearTrackingTable(ctx context.Context) {
	for {
		select {
		// connector is stopping, clear table last time.
		case <-i.tableSrv.stopCh:
			err := i.deleteRows(ctx)
			if err != nil {
				i.tableSrv.errCh <- err
			}

			// query finished, db can be closed.
			i.tableSrv.canCloseCh <- true

			return

		case <-time.After(clearTrackingTableTimeoutSec * time.Second):
			err := i.deleteRows(ctx)
			if err != nil {
				i.tableSrv.errCh <- err

				return
			}
		}
	}
}
