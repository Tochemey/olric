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
	"slices"
	"sync"
)

// ArrayList type that can be safely shared between goroutines.
type ArrayList[T any] struct {
	data []T
	mu   sync.RWMutex
}

// NewArrayList creates a new lock-free thread-safe slice.
func NewArrayList[T any]() *ArrayList[T] {
	return &ArrayList[T]{data: []T{}}
}

// Len returns the number of items
func (x *ArrayList[T]) Len() int {
	x.mu.RLock()
	l := len(x.data)
	x.mu.RUnlock()
	return l
}

// Append adds an item to the concurrent slice.
func (x *ArrayList[T]) Append(item T) {
	x.mu.Lock()
	x.data = append(x.data, item)
	x.mu.Unlock()
}

// AppendMany adds many items to the concurrent slice
func (x *ArrayList[T]) AppendMany(item ...T) {
	x.mu.Lock()
	x.data = append(x.data, item...)
	x.mu.Unlock()
}

// Get returns the slice item at the given index
func (x *ArrayList[T]) Get(index int) (item T) {
	x.mu.RLock()
	if index < 0 || index >= len(x.data) {
		var zero T
		x.mu.RUnlock()
		return zero
	}
	x.mu.RUnlock()
	return x.data[index]
}

// Delete an item from the slice
func (x *ArrayList[T]) Delete(index int) {
	x.mu.Lock()
	if index < 0 || index >= len(x.data) {
		x.mu.Unlock()
		return
	}
	x.data = slices.Delete(x.data, index, index+1)
	x.mu.Unlock()
}

// Items returns the list of items
func (x *ArrayList[T]) Items() []T {
	x.mu.RLock()
	dataCopy := make([]T, len(x.data))
	copy(dataCopy, x.data)
	x.mu.RUnlock()
	return dataCopy
}

// Reset resets the slice
func (x *ArrayList[T]) Reset() {
	x.mu.Lock()
	x.data = []T{}
	x.mu.Unlock()
}
