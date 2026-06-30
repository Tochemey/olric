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

package checkpoint

import "sync/atomic"

var (
	required int32
	passed   int32
)

func Add() {
	atomic.AddInt32(&required, 1)
}

func Pass() {
	atomic.AddInt32(&passed, 1)
}

func AllPassed() bool {
	return atomic.LoadInt32(&passed) == required
}

// Reset resets the checkpoint counters to zero. Call this at the start of each
// test that creates a fresh Olric node to prevent counter accumulation across tests.
func Reset() {
	atomic.StoreInt32(&required, 0)
	atomic.StoreInt32(&passed, 0)
}
