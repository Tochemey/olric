/*
 * Copyright 2018-2024 Burak Sezer
 * Copyright 2025-2026 Arsene Tochemey Gandote
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package collection

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	// create a concurrent slice of integer
	sl := NewArrayList[int]()

	// add some items
	sl.Append(2)
	sl.Append(4)
	sl.Append(5)

	// assert the length
	assert.EqualValues(t, 3, sl.Len())
	assert.NotEmpty(t, sl.Items())
	assert.Len(t, sl.Items(), 3)
	// get the element at index 2
	assert.EqualValues(t, 5, sl.Get(2))
	// remove the element at index 1
	sl.Delete(1)
	// assert the length
	assert.EqualValues(t, 2, sl.Len())
	assert.Zero(t, sl.Get(4))
	sl.Reset()
	assert.Zero(t, sl.Len())
	sl.AppendMany(1, 2, 3, 4, 5)
	assert.EqualValues(t, 5, sl.Len())

	// deleting an item that does not exist should not panic
	sl.Delete(10)
}
