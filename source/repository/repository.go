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

package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
)

// Repository DB2 repository
type Repository struct {
	db sql.DB
}

// GetData get rows with columns offset from table.
func (r *Repository) GetData(
	ctx context.Context,
	table, key string,
	fields []string,
	offset, limit int,
) ([]map[string]interface{}, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	rows, err := r.db.QueryContext(ctx, buildGetDataQuery(table, key, fields, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("run query: %v", err)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %v", err)
	}

	result := make([]map[string]interface{}, 0)

	colValues := make([]interface{}, len(columns))

	for rows.Next() {
		row := make(map[string]interface{}, len(columns))

		for i := range colValues {
			colValues[i] = new(interface{})
		}

		if er := rows.Scan(colValues...); er != nil {
			return nil, fmt.Errorf("scan: %v", err)
		}

		for i, col := range columns {
			row[col] = *colValues[i].(*interface{})
		}

		result = append(result, row)
	}

	return result, nil
}

func buildGetDataQuery(table, key string, fields []string, offset, limit int) string {
	sb := sqlbuilder.NewSelectBuilder()

	if len(fields) == 0 {
		sb.Select("*")
	} else {
		sb.Select(fields...)
	}

	sb.From(table)
	sb.Offset(offset)
	sb.Limit(limit)

	return sb.String()
}
