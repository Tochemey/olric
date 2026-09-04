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
	// Replicate copies every live table of the fragment to the owners, to be
	// merged into the partition of the given kind, and keeps the local copy.
	// It is the transfer behind pushing a primary copy to its replica owners
	// and restoring a primary copy from a backup; Move and MoveWithTargetKind
	// transfer ownership instead and drop what they sent.
	Replicate(*Partition, string, []discovery.Member, Kind) error
	Compaction() (bool, error)
	Destroy() error
	Close() error
}
