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
		name    string
		args    args
		want    Config
		wantErr bool
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
			wantErr: true,
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
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
