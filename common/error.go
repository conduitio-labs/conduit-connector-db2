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

package common

import "fmt"

type LessThanError struct {
	fieldName string
	value     int
}

func (e LessThanError) Error() string {
	return fmt.Sprintf("%q value must be less than or equal to %d", e.fieldName, e.value)
}

func NewLessThanError(fieldName string, value int) LessThanError {
	return LessThanError{fieldName: fieldName, value: value}
}
