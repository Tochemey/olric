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

import "crypto/tls"

// TLS holds the TLS configuration for both client and server components.
// It provides separate configurations to securely establish encrypted communication
// in different roles, ensuring proper TLS settings are used where needed.
type TLS struct {
	// Client contains the TLS configuration for a client connection.
	// This is used when making secure outbound connections to a remote server.
	// It typically includes certificates, root CAs, and other security settings.
	Client *tls.Config

	// Server contains the TLS configuration for a server listener.
	// This is used when accepting incoming secure connections from clients.
	// It includes server certificates, private keys, and CA verification settings.
	Server *tls.Config
}
