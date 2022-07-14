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

package writer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
)

const (
	// metadata related.
	metadataTable  = "table"
	metadataAction = "action"

	// action names.
	actionInsert = "insert"
)

// Writer implements a writer logic for db2 destination.
type Writer struct {
	db        *sql.DB
	table     string
	keyColumn string
}

// Params is an incoming params for the NewWriter function.
type Params struct {
	DB        *sql.DB
	Table     string
	KeyColumn string
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, params Params) *Writer {
	return &Writer{
		db:        params.DB,
		table:     params.Table,
		keyColumn: params.KeyColumn,
	}
}

// InsertRecord inserts a sdk.Record into a Destination.
func (w *Writer) InsertRecord(ctx context.Context, record sdk.Record) error {
	action := record.Metadata[metadataAction]

	// TODO: handle update and delete actions
	switch action {
	case actionInsert:
		return w.insert(ctx, record)
	default:
		return w.insert(ctx, record)
	}
}

// Close closes the underlying db connection.
func (w *Writer) Close(ctx context.Context) error {
	return w.db.Close()
}

// insert is an append-only operation that doesn't care about keys.
func (w *Writer) insert(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrEmptyPayload
	}

	columns, values := w.extractColumnsAndValues(payload)

	query, err := w.buildInsertQuery(tableName, columns, values)
	if err != nil {
		return fmt.Errorf("build insert query: %w", err)
	}

	_, err = w.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("exec insert: %w", err)
	}

	return nil
}

// buildInsertQuery generates an SQL INSERT statement query,
// based on the provided table, columns and values.
func (w *Writer) buildInsertQuery(table string, columns []string, values []any) (string, error) {
	if len(columns) != len(values) {
		return "", ErrColumnsValuesLenMismatch
	}

	return sqlbuilder.InsertInto(table).Cols(columns...).Values(values...).String(), nil
}

// getTableName returns either the records metadata value for table
// or the default configured value for table.
func (w *Writer) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return w.table
	}

	return strings.ToLower(tableName)
}

// getKeyColumn returns either the first key within the Key structured data
// or the default key configured value for key.
func (w *Writer) getKeyColumn(key sdk.StructuredData) (string, error) {
	if len(key) > 1 {
		return "", ErrCompositeKeysNotSupported
	}

	for k := range key {
		return k, nil
	}

	return w.keyColumn, nil
}

// structurizeData converts sdk.Data to sdk.StructuredData.
func (w *Writer) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// extractColumnsAndValues turns the payload into slices of
// columns and values for inserting into db2.
func (w *Writer) extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
	var (
		columns []string
		values  []any
	)

	for key, value := range payload {
		columns = append(columns, key)
		values = append(values, value)
	}

	return columns, values
}
