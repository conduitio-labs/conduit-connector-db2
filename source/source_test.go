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
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-db2/source/config"
	"github.com/conduitio-labs/conduit-connector-db2/source/mock"
	"github.com/conduitio-labs/conduit-connector-db2/source/position"
	"github.com/conduitio/conduit-commons/opencdc"
	"go.uber.org/mock/gomock"
)

func TestSource_Configure(t *testing.T) {
	s := Source{}

	tests := []struct {
		name    string
		cfg     map[string]string
		wantErr bool
	}{
		{
			name: "success, required and default fields",
			cfg: map[string]string{
				config.ConfigConnection:     "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
				config.ConfigTable:          "CLIENTS",
				config.ConfigColumns:        "",
				config.ConfigOrderingColumn: "ID",
				config.ConfigBatchSize:      "",
			},
			wantErr: false,
		},
		{
			name: "success, custom batch size",
			cfg: map[string]string{
				config.ConfigConnection:     "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
				config.ConfigTable:          "CLIENTS",
				config.ConfigColumns:        "",
				config.ConfigOrderingColumn: "ID",
				config.ConfigBatchSize:      "50",
			},
			wantErr: false,
		},
		{
			name: "success, custom columns",
			cfg: map[string]string{
				config.ConfigConnection:     "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
				config.ConfigTable:          "CLIENTS",
				config.ConfigColumns:        "ID,NAME",
				config.ConfigOrderingColumn: "ID",
				config.ConfigBatchSize:      "50",
			},
			wantErr: false,
		},
		{
			name: "failed, missed ordering column",
			cfg: map[string]string{
				config.ConfigConnection: "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=pwd",
				config.ConfigTable:      "CLIENTS",
				config.ConfigColumns:    "ID,NAME",
				config.ConfigBatchSize:  "50",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Configure(context.Background(), tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
		})
	}
}

func TestSource_Read(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		st := make(opencdc.StructuredData)
		st["key"] = "value"

		pos, _ := json.Marshal(position.Position{
			IteratorType:             position.TypeSnapshot,
			SnapshotLastProcessedVal: "1",
			CDCLastID:                0,
		})

		record := opencdc.Record{
			Position: pos,
			Metadata: nil,
			Key:      st,
			Payload:  opencdc.Change{After: st},
		}

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(record, nil)

		s := Source{
			iterator: it,
		}

		r, err := s.Read(ctx)
		if err != nil {
			t.Errorf("read error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(r, record) {
			t.Errorf("got = %v, want %v", r, record)
		}
	})

	t.Run("failed_has_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, errors.New("run query: failed"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})

	t.Run("failed_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(opencdc.Record{}, errors.New("key is not exist"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}

func TestSource_Teardown(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop().Return(nil)

		s := Source{
			iterator: it,
		}
		err := s.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown error = \"%s\"", err.Error())
		}
	})

	t.Run("failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop().Return(errors.New("some error"))

		s := Source{
			iterator: it,
		}

		err := s.Teardown(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}
