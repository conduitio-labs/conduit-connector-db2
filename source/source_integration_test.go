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
// limitations under the License

package source

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/conduitio-labs/conduit-connector-db2/config"
)

func TestSource_Snapshot(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		fmt.Println(err)
	}

	// Check first read.
	r, err := s.Read(ctx)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(r)

	// Check first read.
	r, err = s.Read(ctx)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(r)

	r, err = s.Read(ctx)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(r)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareConfig() (map[string]string, error) {
	connection := os.Getenv("DB2_CONNECTION")

	//if connection == "" {
	//	return map[string]string{}, errors.New("DB2_CONNECTION env var must be set")
	//}

	// TODO remove it
	connection = "HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=DB2INST1;PWD=GD1OJfLGG64HV2dtwK"

	return map[string]string{
		config.KeyConnection: connection,
		config.KeyTable:      "demo",
		config.KeyPrimaryKey: "id",
		KeyOrderingColumn:    "id",
	}, nil
}
