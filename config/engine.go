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

package config

import (
	"fmt"

	"github.com/tochemey/olric/internal/kvstore"
	"github.com/tochemey/olric/pkg/storage"
)

// Engine contains storage engine configuration and their implementations.
// If you don't have a custom storage engine implementation or configuration for
// the default one, just call NewStorageEngine() function to use it with sane defaults.
type Engine struct {
	Name string

	Storage storage.Engine

	// Config is a map that contains configuration of the storage engines, for
	// both plugins and imported ones. If you want to use a storage engine other
	// than the default one, you must set configuration for it.
	Config map[string]any
}

// NewEngine initializes Engine with sane defaults.
// Olric will set its own storage engine implementation and related configuration,
// if there is no other engine.
func NewEngine() *Engine {
	return &Engine{
		Config: make(map[string]any),
	}
}

// TableSize returns the configured storage table size in bytes for this engine.
// It reads the "tableSize" entry from Config, falling back to the engine default
// when the entry is absent, of an unexpected type, or when the receiver or its
// Config is nil. This mirrors how the kvstore engine resolves its table size, so
// it is safe to call before Sanitize has populated defaults.
func (s *Engine) TableSize() uint64 {
	if s == nil || s.Config == nil {
		return kvstore.DefaultTableSize()
	}

	raw, ok := s.Config["tableSize"]
	if !ok {
		return kvstore.DefaultTableSize()
	}

	size, err := kvstore.PrepareTableSize(raw)
	if err != nil {
		return kvstore.DefaultTableSize()
	}

	return size
}

// Validate finds errors in the current configuration.
func (s *Engine) Validate() error {
	if s.Config == nil {
		s.Config = make(map[string]any)
	}
	return nil
}

// Sanitize sets default values to empty configuration variables, if it's possible.
func (s *Engine) Sanitize() error {
	if s.Name == "" {
		s.Name = DefaultStorageEngine
	}

	if s.Storage == nil {
		switch s.Name {
		case DefaultStorageEngine:
			cfg := kvstore.DefaultConfig().ToMap()
			for key, value := range cfg {
				_, ok := s.Config[key]
				if !ok {
					s.Config[key] = value
				}
			}
			kv, err := kvstore.New(storage.NewConfig(s.Config))
			if err != nil {
				return err
			}
			s.Storage = kv
		default:
			return fmt.Errorf("unknown storage engine: %s", s.Name)
		}
	} else {
		s.Name = s.Storage.Name()
	}
	return nil
}

// Interface guard
var _ IConfig = (*Engine)(nil)
