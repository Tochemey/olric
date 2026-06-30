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

package redislogger

import (
	"context"
	"testing"
)

func TestDiscardLogger_Printf(t *testing.T) {
	// DiscardLogger.Printf must be a no-op and must never panic regardless of
	// the arguments it receives.
	logger := &DiscardLogger{}

	// Nil context, empty format, no args.
	logger.Printf(context.Background(), "")

	// A format string with arguments.
	logger.Printf(context.Background(), "value=%d name=%s", 42, "olric")

	// A nil context must be tolerated as well.
	logger.Printf(context.TODO(), "no args here")
}
