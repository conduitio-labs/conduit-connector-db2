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

	"github.com/conduitio/conduit-commons/opencdc"
)

const (

	// DB2 Types.

	// String types.
	charType           = "CHARACTER"
	clobType           = "CLOB"
	longVarcharType    = "LONG VARCHAR"
	graphicType        = "GRAPHIC"
	varcharType        = "VARCHAR"
	longVarGraphicType = "LONG VARGRAPHIC"
	varGraphicType     = "VARGRAPHIC"
	decimalType        = "DECIMAL"
	decimalFloatType   = "DECFLOAT"
	dbClobType         = "DBCLOB"

	// Time types.
	dateType  = "DATE"
	timeType  = "TIME"
	timeStamp = "TIMESTAMP"

	// Binary types.
	binaryType    = "BINARY"
	varbinaryType = "VARBINARY"
	blobType      = "BLOB"
)

var (
	// querySchemaColumnTypes is a query that selects column names and
	// their data and column types from the information_schema.
	querySchemaColumnTypes = `
			SELECT 
				   colname AS column_name,
				   typename AS data_type,
				   length,			   
				   keyseq 
			FROM syscat.columns
			WHERE tabname = '%s'
`
	// time layouts.
	layouts = []string{time.RFC3339, time.RFC3339Nano, time.Layout, time.ANSIC, time.UnixDate, time.RubyDate,
		time.RFC822, time.RFC822Z, time.RFC850, time.RFC1123, time.RFC1123Z, time.RFC3339, time.RFC3339,
		time.RFC3339Nano, time.Kitchen, time.Stamp, time.StampMilli, time.StampMicro, time.StampNano}

	// column types where length is required parameter.
	typesWithLength = []string{charType, varcharType, clobType, graphicType, varGraphicType, dbClobType,
		binaryType, varbinaryType, blobType}
)

// Querier is a database querier interface needed for the GetTableInfo function.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// TableInfo - information about colum types, primary keys from table.
type TableInfo struct {
	// ColumnTypes - column name with column type.
	ColumnTypes map[string]string
	// ColumnLengths - column name with length
	ColumnLengths map[string]int
	// PrimaryKeys - primary keys column names.
	PrimaryKeys []string
}

func (t TableInfo) GetCreateColumnStr() string {
	var columns []string
	for key, val := range t.ColumnTypes {
		cl := fmt.Sprintf("%s %s", key, val)
		if isTypeWithRequiredLength(val) {
			cl = fmt.Sprintf("%s(%d)", cl, t.ColumnLengths[key])
		}

		columns = append(columns, cl)
	}

	return strings.Join(columns, ",")
}

func isTypeWithRequiredLength(elem string) bool {
	for _, val := range typesWithLength {
		if val == elem {
			return true
		}
	}

	return false
}

// TransformRow converts row map values to appropriate Go types, based on the columnTypes.
func TransformRow(_ context.Context, row map[string]any, columnTypes map[string]string) (map[string]any, error) {
	result := make(map[string]any, len(row))

	for key, value := range row {
		if value == nil {
			result[key] = value

			continue
		}

		switch columnTypes[key] {
		// Convert to string.
		case charType, clobType, longVarcharType, graphicType, longVarGraphicType,
			varcharType, varGraphicType, decimalType, decimalFloatType:
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
	_ context.Context,
	columnTypes map[string]string,
	data opencdc.StructuredData,
) (opencdc.StructuredData, error) {
	result := make(opencdc.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = value

			continue
		}

		// DB2 doesn't have json type or similar.
		// DB2 string types can replace it.
		switch reflect.TypeOf(value).Kind() {
		case reflect.Map, reflect.Slice:
			bs, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("marshal: %w", err)
			}

			result[key] = string(bs)

			continue
		}

		// Converting value to time if it is string.
		switch columnTypes[strings.ToUpper(key)] {
		case dateType, timeType, timeStamp:
			_, ok := value.(time.Time)
			if ok {
				result[key] = value

				continue
			}

			valueStr, ok := value.(string)
			if !ok {
				return nil, ErrValueIsNotAString
			}

			timeValue, err := parseTime(valueStr)
			if err != nil {
				return nil, fmt.Errorf("convert value to time.Time: %w", err)
			}

			result[key] = timeValue
		// DecimalFloat must be a number.
		case decimalFloatType:
			switch v := value.(type) {
			case float64:
				result[key] = v
			case float32:
				result[key] = v
			case int64:
				result[key] = value.(float64)
			case int32:
				result[key] = value.(float32)
			case string:
				res, err := strconv.ParseFloat(value.(string), 64)
				if err != nil {
					return nil, fmt.Errorf("parse float: %w", err)
				}

				result[key] = res
			default:
				return nil, ErrConvertDecFloat
			}

		case binaryType, varbinaryType, blobType:
			_, ok := value.([]byte)
			if ok {
				result[key] = value

				continue
			}

			valueStr, ok := value.(string)
			if !ok {
				return nil, ErrValueIsNotAString
			}

			result[key] = []byte(valueStr)

		default:
			result[key] = value
		}
	}

	return result, nil
}

// GetTableInfo returns a map containing all table's columns and their database types
// and returns primary columns names.
func GetTableInfo(ctx context.Context, querier Querier, tableName string) (TableInfo, error) {
	rows, err := querier.QueryContext(ctx, fmt.Sprintf(querySchemaColumnTypes, tableName))
	if err != nil {
		return TableInfo{}, fmt.Errorf("query column types: %w", err)
	}

	defer rows.Close()

	columnTypes := make(map[string]string)
	columnLengths := make(map[string]int)
	primaryKeys := make([]string, 0)

	for rows.Next() {
		var (
			columnName, dataType string
			length               int
			keyseq               *int
		)
		if er := rows.Scan(&columnName, &dataType, &length, &keyseq); er != nil {
			return TableInfo{}, fmt.Errorf("scan rows: %w", er)
		}

		columnTypes[columnName] = dataType
		columnLengths[columnName] = length

		// check is it primary key.
		if keyseq != nil && *keyseq == 1 {
			primaryKeys = append(primaryKeys, columnName)
		}
	}
	if err := rows.Err(); err != nil {
		return TableInfo{}, fmt.Errorf("error iterating rows: %w", err)
	}

	return TableInfo{
		ColumnTypes:   columnTypes,
		PrimaryKeys:   primaryKeys,
		ColumnLengths: columnLengths,
	}, nil
}

func parseTime(val string) (time.Time, error) {
	for _, l := range layouts {
		timeValue, err := time.Parse(l, val)
		if err != nil {
			continue
		}

		return timeValue, nil
	}

	return time.Time{}, fmt.Errorf("%s - %w", val, ErrInvalidTimeLayout)
}
