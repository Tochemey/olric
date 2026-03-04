/*
 * Copyright 2018-2024 Burak Sezer
 * Copyright 2025 Arsene Tochemey Gandote
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

package partitions

import (
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/pkg/storage"
)

type Fragment interface {
	Name() string
	Stats() storage.Stats
	Move(*Partition, string, []discovery.Member) error
	// MoveWithTargetKind is like Move but uses targetKind in the payload so the
	// receiver merges into the correct partition (e.g. BACKUP when pushing from primary).
	MoveWithTargetKind(*Partition, string, []discovery.Member, Kind) error
	Compaction() (bool, error)
	Destroy() error
	Close() error
}
