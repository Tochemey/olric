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

// Checkpoint tracks the readiness of the components of a single Olric
// instance. Counters are per-instance so a failed start of one node cannot
// block the Started signal of another node in the same process.
type Checkpoint struct {
	required int32
	passed   int32
}

func New() *Checkpoint {
	return &Checkpoint{}
}

func (c *Checkpoint) Add() {
	atomic.AddInt32(&c.required, 1)
}

func (c *Checkpoint) Pass() {
	atomic.AddInt32(&c.passed, 1)
}

func (c *Checkpoint) AllPassed() bool {
	return atomic.LoadInt32(&c.passed) == atomic.LoadInt32(&c.required)
}
