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

//go:generate paramgen -output=paramgen.go Configuration

package common

import (
	"strings"
)

const MaxConfigStringLength = 128

// Config contains configurable values
// shared between source and destination DB2 connector.
type Configuration struct {
	// Connection string connection to DB2 database.
	Connection string `json:"connection" validate:"required"`
	// Table is a name of the table that the connector should write to or read from.
	Table string `json:"table" validate:"required"`
}

// Init sets uppercase "table" name.
func (c Configuration) Init() Configuration {
	c.Table = strings.ToUpper(c.Table)

	return c
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Configuration) Validate() error {
	if len(c.Table) > MaxConfigStringLength {
		return NewLessThanError(ConfigurationTable, MaxConfigStringLength)
	}

	return nil
}
