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

// Package coltypes implements functions for converting DB2 column types to appropriate Go types.
package coltypes

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (

	// DB2 Types.
	charType           = "CHARACTER"
	clobType           = "CLOB"
	longVarcharType    = "LONG VARCHAR"
	graphicType        = "GRAPHIC"
	varcharType        = "VARCHAR"
	longVarGraphicType = "LONG VARGRAPHIC"
	varGraphicType     = "VARGRAPHIC"
	decimalType        = "DECIMAL"
	decimalFloat       = "DECFLOAT"

	date      = "DATE"
	timeType  = "TIME"
	timeStamp = "TIMESTAMP"
)

var (
	// querySchemaColumnTypes is a query that selects column names and
	// their data and column types from the information_schema.
	querySchemaColumnTypes = `
			SELECT 
				   colname as column_name,
				   typename as data_type
			from syscat.columns
			where tabname = '%s'
`
)

// Querier is a database querier interface needed for the GetColumnTypes function.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// TransformRow converts row map values to appropriate Go types, based on the columnTypes.
func TransformRow(ctx context.Context, row map[string]any, columnTypes map[string]string) (map[string]any, error) {
	result := make(map[string]any, len(row))

	for key, value := range row {
		if value == nil {
			result[key] = value

			continue
		}

		switch columnTypes[key] {
		case charType, clobType, longVarcharType, graphicType, longVarGraphicType,
			varcharType, varGraphicType, decimalType, decimalFloat:
			valueBytes, ok := value.([]byte)
			if !ok {
				return nil, convertValueToBytesErr(key)
			}

			result[key] = string(valueBytes)

		default:
			result[key] = value
		}
	}

	return result, nil
}

// ConvertStructureData converts a sdk.StructureData values to a proper database types.
func ConvertStructureData(
	ctx context.Context,
	columnTypes map[string]string,
	data sdk.StructuredData,
) (sdk.StructuredData, error) {
	result := make(sdk.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = value

			continue
		}

		// DB2 doesn't have json or similar column types.
		// For this case it is better use  some string type column.
		switch reflect.TypeOf(value).Kind() {
		case reflect.Map, reflect.Slice:
			bs, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("marshal: %w", err)
			}

			result[key] = string(bs)

			continue
		}

		switch columnTypes[strings.ToUpper(key)] {
		// Converting value to time.Time for date, time, timestamp DB2 columns.
		case date, timeType, timeStamp:
			valueStr, ok := value.(string)
			if !ok {
				return nil, ErrValueIsNotAString
			}

			timeValue, err := time.Parse(time.RFC3339, valueStr)
			if err != nil {
				return nil, fmt.Errorf("convert value to time.Time: %w", err)
			}

			result[key] = timeValue
		// DecimalFlot must be number.
		case decimalFloat:
			switch value.(type) {
			case float64:
				result[key] = value
			case float32:
				result[key] = value
			case int64:
				result[key] = value.(float64)
			case string:
				res, err := strconv.ParseFloat(value.(string), 64)
				if err != nil {
					return nil, fmt.Errorf("parse float: %w", err)
				}

				result[key] = res
			default:
				return nil, ErrConvertDecFloat
			}

		default:
			result[key] = value
		}
	}

	return result, nil
}

// GetColumnTypes returns a map containing all table's columns and their database types.
func GetColumnTypes(ctx context.Context, querier Querier, tableName string) (map[string]string, error) {
	rows, err := querier.QueryContext(ctx, fmt.Sprintf(querySchemaColumnTypes, tableName))
	if err != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}

	columnTypes := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		if er := rows.Scan(&columnName, &dataType); er != nil {
			return nil, fmt.Errorf("scan rows: %w", er)
		}

		columnTypes[columnName] = dataType
	}

	return columnTypes, nil
}
