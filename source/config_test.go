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

package source

import (
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-db2/config"
)

func TestParse(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}
	tests := []struct {
		name        string
		args        args
		want        Config
		wantErr     bool
		expectedErr string
	}{
		{
			name: "success, required and default fields",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					config.KeyTable:      "CLIENTS",
					KeyColumns:           "",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				BatchSize:      defaultBatchSize,
				Snapshot:       true,
			},
			wantErr: false,
		},
		{
			name: "success, custom batch size",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					config.KeyTable:      "CLIENTS",
					KeyColumns:           "",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "50",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				BatchSize:      50,
				Snapshot:       true,
			},
			wantErr: false,
		},
		{
			name: "success, custom columns",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					config.KeyTable:      "CLIENTS",
					KeyColumns:           "id,name",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "50",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				BatchSize:      50,
				Columns:        []string{"ID", "NAME"},
				Snapshot:       true,
			},
			wantErr: false,
		},
		{
			name: "success, custom keys",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					config.KeyTable:      "CLIENTS",
					KeyOrderingColumn:    "ID",
					KeyPrimaryKeys:       "id",
					KeyBatchSize:         "50",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				PrimaryKeys:    []string{"ID"},
				BatchSize:      50,
				Snapshot:       true,
			},
			wantErr: false,
		},
		{
			name: "success, custom snapshot",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					config.KeyTable:      "CLIENTS",
					KeyOrderingColumn:    "ID",
					KeyPrimaryKeys:       "id",
					KeyBatchSize:         "50",
					KeySnapshot:          "false",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				PrimaryKeys:    []string{"ID"},
				BatchSize:      50,
				Snapshot:       false,
			},
			wantErr: false,
		},
		{
			name: "failed, missed ordering column",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					config.KeyTable:      "CLIENTS",
					KeyColumns:           "ID,NAME",
					KeyBatchSize:         "50",
				},
			},
			wantErr:     true,
			expectedErr: `validate source config: "orderingColumn" value must be set; "columns" value must contains values of these fields: "OrderingColumn"`, //nolint:lll // for test purpose
		},
		{
			name: "failed, missed ordering column in custom columns",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					config.KeyTable:      "CLIENTS",
					KeyColumns:           "AGE,NAME",
					KeyBatchSize:         "50",
					KeyOrderingColumn:    "ID",
				},
			},
			wantErr:     true,
			expectedErr: `validate source config: "columns" value must contains values of these fields: "OrderingColumn"`,
		},
		{
			name: "failed, invalid snapshot mode",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
					config.KeyTable:      "CLIENTS",
					KeyColumns:           "AGE,NAME",
					KeyBatchSize:         "50",
					KeyOrderingColumn:    "ID",
					KeySnapshot:          "mode",
				},
			},
			wantErr:     true,
			expectedErr: `parse "snapshot": strconv.ParseBool: parsing "mode": invalid syntax`,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.args.cfg)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr, err.Error())

					return
				}

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}
