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
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-db2/coltypes"
	"github.com/conduitio-labs/conduit-connector-db2/source/position"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

const (
	waitingTimeoutSec            = 20
	clearTrackingTableTimeoutSec = 5
)

// trackingTableService service for clearing tracking table.
type trackingTableService struct {
	m sync.Mutex

	// channel for getting stop signal.
	stopCh chan struct{}
	// channel for errors.
	errCh chan error
	// channel for notify that all queries finished and db can be closed.
	canCloseCh chan struct{}
	// idsForRemoving - ids of rows what need to clear.
	idsForRemoving []any
}

func newTrackingTableService() *trackingTableService {
	return &trackingTableService{
		stopCh:     make(chan struct{}, 1),
		errCh:      make(chan error, 1),
		canCloseCh: make(chan struct{}, 1),
	}
}

func (t *trackingTableService) close() {
	close(t.canCloseCh)
	close(t.errCh)
	close(t.stopCh)
}

// cdcIterator - cdc iterator.
type cdcIterator struct {
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
	// keys Names of columns what iterator use for setting key in record.
	keys []string
	// batchSize size of batch.
	batchSize int
	// position last recorded position.
	position *position.Position
	// columnTypes column types from table.
	columnTypes map[string]string
}

type cdcParams struct {
	db            *sqlx.DB
	table         string
	trackingTable string
	keys          []string
	columns       []string
	batchSize     int
	columnTypes   map[string]string
	position      *position.Position
}

// newCDCIterator create new cdc iterator.
func newCDCIterator(
	ctx context.Context,
	params cdcParams,
) (*cdcIterator, error) {
	var (
		err error
	)

	it := &cdcIterator{
		db:            params.db,
		table:         params.table,
		trackingTable: params.trackingTable,
		columns:       params.columns,
		keys:          params.keys,
		batchSize:     params.batchSize,
		position:      params.position,
		columnTypes:   params.columnTypes,
		tableSrv:      newTrackingTableService(),
	}

	if err = it.loadRows(ctx); err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	// run clearing tracking table.
	go it.clearTrackingTable(ctx)

	return it, nil
}

// HasNext check ability to get next record.
func (i *cdcIterator) HasNext(ctx context.Context) (bool, error) {
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return false, nil
}

// Next get new record.
func (i *cdcIterator) Next(ctx context.Context) (opencdc.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return opencdc.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := coltypes.TransformRow(ctx, row, i.columnTypes)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	id, ok := transformedRow[columnTrackingID].(int32)
	if !ok {
		return opencdc.Record{}, ErrWrongTrackingIDType
	}

	operationTypeBt, ok := transformedRow[columnOperationType].([]byte)
	if !ok {
		return opencdc.Record{}, ErrWrongTrackingOperatorType
	}

	operationType := string(operationTypeBt)

	pos := position.Position{
		IteratorType: position.TypeCDC,
		CDCLastID:    int(id),
		SuffixName:   i.trackingTable[len(i.trackingTable)-6:],
	}

	convertedPosition, err := pos.ConvertToSDKPosition()
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

	// delete tracking columns
	delete(transformedRow, columnOperationType)
	delete(transformedRow, columnTrackingID)
	delete(transformedRow, columnTimeCreated)

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	i.position = &pos

	metadata := opencdc.Metadata(map[string]string{metadataTable: i.table})
	metadata.SetCreatedAt(time.Now())

	switch actionType(operationType) {
	case ActionInsert:
		return sdk.Util.Source.NewRecordCreate(convertedPosition, metadata,
			opencdc.StructuredData(keysMap), opencdc.RawData(transformedRowBytes)), nil
	case ActionUpdate:
		return sdk.Util.Source.NewRecordUpdate(convertedPosition, metadata,
			opencdc.StructuredData(keysMap), nil, opencdc.RawData(transformedRowBytes)), nil
	case ActionDelete:
		return sdk.Util.Source.NewRecordDelete(convertedPosition, metadata,
			opencdc.StructuredData(keysMap), nil), nil
	default:
		return opencdc.Record{}, ErrUnknownOperatorType
	}
}

// Stop shutdown iterator.
func (i *cdcIterator) Stop() error {
	// send signal for finish clear tracking table.
	i.tableSrv.stopCh <- struct{}{}

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
			i.tableSrv.close()

			return i.db.Close()
		}
	// waiting timeout.
	case <-time.After(waitingTimeoutSec * time.Second):
		if i.db != nil {
			i.tableSrv.close()

			return i.db.Close()
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (i *cdcIterator) Ack(_ context.Context, pos *position.Position) error {
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
func (i *cdcIterator) loadRows(ctx context.Context) error {
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
func (i *cdcIterator) deleteRows(ctx context.Context) error {
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

func (i *cdcIterator) clearTrackingTable(ctx context.Context) {
	for {
		select {
		// connector is stopping, clear table last time.
		case <-i.tableSrv.stopCh:
			err := i.deleteRows(ctx)
			if err != nil {
				i.tableSrv.errCh <- err
			}

			// clearing was finished, db can be closed.
			i.tableSrv.canCloseCh <- struct{}{}

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

// setupCDC - create tracking table, add columns.
func setupCDC(
	ctx context.Context,
	db *sqlx.DB,
	tableName, trackingTableName, suffixName string,
	tableInfo coltypes.TableInfo,
) error {
	var (
		trackingTableExist bool
	)

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	// check if table exist.
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(queryIfExistTable, trackingTableName))
	if err != nil {
		return fmt.Errorf("query exist table: %w", err)
	}

	defer rows.Close() //nolint:staticcheck,nolintlint

	for rows.Next() {
		var count int
		er := rows.Scan(&count)
		if er != nil {
			return fmt.Errorf("scan: %w", er)
		}

		if count == 1 {
			trackingTableExist = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	if !trackingTableExist {
		// create tracking table
		_, err = tx.ExecContext(ctx, fmt.Sprintf(queryCreateTable, trackingTableName, tableInfo.GetCreateColumnStr(),
			columnOperationType, columnTimeCreated, columnTrackingID))
		if err != nil {
			return fmt.Errorf("create tracking table: %w", err)
		}
	}

	triggersQuery := buildTriggers(trackingTableName, tableName, suffixName, tableInfo.ColumnTypes)

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
