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
	"io"
	"log"
	"net"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/tochemey/olric/hasher"
)

// IConfig is an interface that has to be implemented by Config and its nested
// structs. It provides a clear and granular way to sanitize and validate
// the configuration.
type IConfig interface {
	// Sanitize methods should be used to set defaults.
	Sanitize() error

	// Validate method should be used to find configuration errors.
	Validate() error
}

const (
	// SyncReplicationMode enables sync replication mode which means that the
	// caller is blocked until write/delete operation is applied by replica
	// owners. The default mode is SyncReplicationMode
	SyncReplicationMode = 0

	// AsyncReplicationMode enables async replication mode which means that
	// write/delete operations are done in a background task.
	AsyncReplicationMode = 1
)

const (
	LogLevelDebug = "DEBUG"
	LogLevelWarn  = "WARN"
	LogLevelError = "ERROR"
	LogLevelInfo  = "INFO"
)

const (
	// DefaultPort is for Olric
	DefaultPort = 3320

	// DefaultDiscoveryPort is for memberlist
	DefaultDiscoveryPort = 3322

	// DefaultPartitionCount denotes default partition count in the cluster.
	DefaultPartitionCount = 271

	// DefaultLoadFactor is used by the consistent hashing function. Keep it small.
	DefaultLoadFactor = 1.25

	// DefaultLogLevel determines the log level without extra configuration.
	// It's DEBUG.
	DefaultLogLevel = LogLevelDebug

	// DefaultLogVerbosity denotes default log verbosity level.
	//
	// * flog.V(1) - Generally useful for this to ALWAYS be visible to an operator
	//   * Programmer errors
	//   * Logging extra info about a panic
	//   * CLI argument handling
	// * flog.V(2) - A reasonable default log level if you don't want verbosity.
	//   * Information about config (listening on X, watching Y)
	//   * Errors that repeat frequently that relate to conditions that can be
	//     corrected (pod detected as unhealthy)
	// * flog.V(3) - Useful steady state information about the service and
	//     important log messages that may correlate to
	//   significant changes in the system.  This is the recommended default log
	//     level for most systems.
	//   * Logging HTTP requests and their exit code
	//   * System state changing (killing pod)
	//   * Controller state change events (starting pods)
	//   * Scheduler log messages
	// * flog.V(4) - Extended information about changes
	//   * More info about system state changes
	// * flog.V(5) - Debug level verbosity
	//   * Logging in particularly thorny parts of code where you may want to come
	//     back later and check it
	// * flog.V(6) - Trace level verbosity
	//   * Context to understand the steps leading up to neterrors and warnings
	//   * More information for troubleshooting reported issues
	DefaultLogVerbosity = 3

	// MinimumReplicaCount denotes default and minimum replica count in an Olric
	// cluster.
	MinimumReplicaCount = 1

	// DefaultBootstrapTimeout denotes default timeout value to check bootstrapping
	// status.
	DefaultBootstrapTimeout = 10 * time.Second

	// DefaultJoinRetryInterval denotes a time gap between sequential join attempts.
	DefaultJoinRetryInterval = time.Second

	// DefaultMaxJoinAttempts denotes a maximum number of failed join attempts
	// before forming a standalone cluster.
	DefaultMaxJoinAttempts = 10

	// DefaultRejoinInterval is the time between attempts to rejoin the cluster
	// when a node detects it is in a minority partition.
	DefaultRejoinInterval = 5 * time.Second

	// DefaultReplicaRestoreDelay is the time the survivors wait after a
	// departure before they re-create the departed member's primary copies
	// from their backups, long enough for a rolling restart to bring the
	// member back first.
	DefaultReplicaRestoreDelay = time.Minute

	// MinimumMemberCountQuorum denotes minimum required count of members to form
	// a cluster.
	MinimumMemberCountQuorum = 1

	// DefaultLRUSamples is a sane default for randomly selected keys
	// in approximate LRU implementation. It's 5.
	DefaultLRUSamples int = 5

	// LRUEviction assigns this as EvictionPolicy in order to enable LRU eviction
	// algorithm.
	LRUEviction EvictionPolicy = "LRU"

	// DefaultStorageEngine denotes the storage engine implementation provided by
	// Olric project.
	DefaultStorageEngine = "kvstore"

	// DefaultRoutingTablePushInterval is interval between routing table push events.
	DefaultRoutingTablePushInterval = time.Minute

	// DefaultTriggerBalancerInterval is interval between two sequential call of balancer worker.
	DefaultTriggerBalancerInterval = 15 * time.Second

	// DefaultCheckEmptyFragmentsInterval is the default value of interval between
	// two sequential call of empty fragment cleaner. It's one minute by default.
	DefaultCheckEmptyFragmentsInterval = time.Minute

	// DefaultTriggerCompactionInterval is the default value of interval between
	// two sequential call of compaction workers. The compaction worker works until
	// its work is done. It's 10 minutes by default.
	DefaultTriggerCompactionInterval = 10 * time.Minute

	// DefaultLeaveTimeout is the default value of maximum amount of time before
	DefaultLeaveTimeout = 5 * time.Second

	DefaultReadQuorum        = 1
	DefaultWriteQuorum       = 1
	DefaultMemberCountQuorum = 1

	// DefaultKeepAlivePeriod is the default value of TCP keepalive. It's 300 seconds.
	// This option is useful in order to detect dead peers (clients that cannot
	// be reached even if they look connected). Moreover, if there is network
	// equipment between clients and servers that need to see some traffic in
	// order to take the connection open, the option will prevent unexpected
	// connection closed events.
	DefaultKeepAlivePeriod = 300 * time.Second
)

// Config is the configuration to create a Olric instance.
type Config struct {
	// Interface denotes a binding interface. It can be used instead of BindAddr
	// if the interface is known but not the address. If both are provided, then
	// Olric verifies that the interface has the bind address that is provided.
	Interface string

	// LogVerbosity denotes the level of message verbosity. The default value
	// is 3. Valid values are between 1 to 6.
	LogVerbosity int32

	// Default LogLevel is DEBUG. Available levels: "DEBUG", "WARN", "ERROR", "INFO"
	LogLevel string

	// BindAddr denotes the address that Olric will bind to for communication
	// with other Olric nodes.
	BindAddr string

	// BindPort denotes the address that Olric will bind to for communication
	// with other Olric nodes.
	BindPort int

	// Client denotes configuration for TCP clients in Olric and the official
	// Golang client.
	Client *Client

	// KeepAlivePeriod denotes whether the operating system should send
	// keep-alive messages on the connection.
	KeepAlivePeriod time.Duration

	// IdleClose will automatically close idle connections after the specified duration.
	// Use zero to disable this feature.
	IdleClose time.Duration

	// Timeout for bootstrap control
	//
	// An Olric node checks operation status before taking any action for the
	// cluster events, responding incoming requests and running API functions.
	// Bootstrapping status is one of the most important checkpoints for an
	// "operable" Olric node. BootstrapTimeout sets a deadline to check
	// bootstrapping status without blocking indefinitely.
	BootstrapTimeout time.Duration

	// Coordinator member pushes the routing table to cluster members in the case of
	// node join or left events. It also pushes the table periodically. RoutingTablePushInterval
	// is the interval between subsequent calls. Default is 1 minute.
	RoutingTablePushInterval time.Duration

	// TriggerBalancerInterval is interval between two sequential call of balancer worker.
	TriggerBalancerInterval time.Duration

	// The list of host:port which are used by memberlist for discovery.
	// Don't confuse it with Name.
	Peers []string

	// PartitionCount is 271, by default.
	PartitionCount uint64

	// ReplicaCount is 1, by default.
	ReplicaCount int

	// Minimum number of successful reads to return a response for a read request.
	ReadQuorum int

	// Minimum number of successful writes to return a response for a write request.
	WriteQuorum int

	// Minimum number of members to form a cluster and run any query on the cluster.
	MemberCountQuorum int32

	// Switch to control read-repair algorithm which helps to reduce entropy.
	ReadRepair bool

	// EnableProactiveSyncOnJoin when true, existing primary owners immediately push
	// their data to new backup owners when a node joins, instead of waiting for the
	// periodic balancer tick. This restores replica redundancy faster after rolling
	// restarts or scale-out events. It has no effect when ReplicaCount is 1.
	// Default is false.
	EnableProactiveSyncOnJoin bool

	// InitialSyncEmptyPartitionTimeout is the max time to wait for an owned but
	// empty partition whose data has not arrived yet. A member first asks the
	// partition's other owners for their key counts and does not wait at all
	// when none of them holds data; the timeout covers owners that hold data
	// but never deliver it and owners that cannot be reached. Default 15s.
	InitialSyncEmptyPartitionTimeout time.Duration

	// ReplicaRestoreDelay is how long the survivors wait, after the primary
	// owner of a partition left the cluster, before they re-create its primary
	// copy from their backup copies on the new owner. The delay is counted per
	// partition from the departure, so a table change in the meantime does
	// not move it. A member that leaves and comes back within the delay, as
	// in a rolling restart, owns its partitions again by then and receives
	// them from their previous owner, so nothing is restored for it. The
	// delay is checked on the balancer tick, so a value of a millisecond
	// restores on the first tick after the departure. It applies only when
	// EnableProactiveSyncOnJoin is set and ReplicaCount is greater than 1, and
	// must not be negative. Default 1m.
	ReplicaRestoreDelay time.Duration

	// Default value is SyncReplicationMode.
	ReplicationMode int

	// LoadFactor is used by consistent hashing function. It determines the maximum
	// load for a server in the cluster. Keep it small.
	LoadFactor float64

	// Olric can send push cluster events to cluster.events channel. Available cluster events:
	//
	// * node-join-event
	// * node-left-event
	// * fragment-migration-event
	// * fragment-received-event
	//
	// If you want to receive these events, set true to EnableClusterEventsChannel and subscribe to
	// cluster.events channel. Default is false.
	EnableClusterEventsChannel bool

	// Default hasher is github.com/cespare/xxhash/v2
	Hasher hasher.Hasher

	// LogOutput is the writer where logs should be sent. If this is not
	// set, logging will go to stderr by default. You cannot specify both LogOutput
	// and Logger at the same time.
	LogOutput io.Writer

	// Logger is a custom logger which you provide. If Logger is set, it will use
	// this for the internal logger. If Logger is not set, it will fall back to the
	// behavior for using LogOutput. You cannot specify both LogOutput and Logger
	// at the same time.
	Logger *log.Logger

	// DMaps denotes a global configuration for DMaps. You can still overwrite it
	// by setting a DMap for a particular distributed map via DMaps.Custom field.
	// Most of the fields are related with distributed cache implementation.
	DMaps *DMaps

	// JoinRetryInterval is the time gap between attempts to join an existing
	// cluster.
	JoinRetryInterval time.Duration

	// MaxJoinAttempts denotes the maximum number of attempts to join an existing
	// cluster before forming a new one.
	MaxJoinAttempts int

	// RejoinInterval is the time between attempts to rejoin the cluster when
	// the node detects it is in a minority partition. It is only effective when
	// MemberCountQuorum is greater than MinimumMemberCountQuorum. Default is 5s.
	RejoinInterval time.Duration

	// Callback function. Olric calls this after
	// the server is ready to accept new connections.
	Started func()

	// ServiceDiscovery is a map that contains plugins implement ServiceDiscovery
	// interface. See pkg/service_discovery/service_discovery.go for details.
	ServiceDiscovery map[string]any

	// Interface denotes a binding interface. It can be used instead of
	// memberlist.Loader.BindAddr if the interface is known but not the address.
	// If both are provided, then Olric verifies that the interface has the bind
	// address that is provided.
	MemberlistInterface string

	// Olric will broadcast a leave message but will not shut down the background
	// listeners, meaning the node will continue participating in gossip and state
	// updates.
	//
	// Sending a leave message will block until the leave message is successfully
	// broadcast to a member of the cluster, if any exist or until a specified timeout
	// is reached.
	LeaveTimeout time.Duration

	// MemberlistConfig is the memberlist configuration that Olric will
	// use to do the underlying membership management and gossip. Some
	// fields in the MemberlistConfig will be overwritten by Olric no
	// matter what:
	//
	//   * Name - This will always be set to the same as the NodeName
	//     in this configuration.
	//
	//   * ClusterEvents - Olric uses a custom event delegate.
	//
	//   * Delegate - Olric uses a custom delegate.
	//
	// You have to use NewMemberlistConfig to create a new one.
	// Then, you may need to modify it to tune for your environment.
	MemberlistConfig *memberlist.Config

	// TLS is used to configure both the TLS Server and Client
	// Ensure that both the Server and Client are configured with the same
	// root Certificate Authority (CA) to enable successful handshake and
	// mutual authentication.
	TLS *TLS

	// MemberMeta is a string that is used to store metadata about the member.
	// The metadata is not used internally by Olric, but is available to the
	// user to store information about the member.
	MemberMeta string
}

// Validate finds errors in the current configuration.
func (c *Config) Validate() error {
	if c.ReplicaCount < MinimumReplicaCount {
		return fmt.Errorf("cannot specify ReplicaCount smaller than MinimumReplicaCount")
	}

	if c.ReadQuorum <= 0 {
		return fmt.Errorf("cannot specify ReadQuorum less than or equal to zero")
	}
	if c.ReplicaCount < c.ReadQuorum {
		return fmt.Errorf("cannot specify ReadQuorum greater than ReplicaCount")
	}

	if c.WriteQuorum <= 0 {
		return fmt.Errorf("cannot specify WriteQuorum less than or equal to zero")
	}
	if c.ReplicaCount < c.WriteQuorum {
		return fmt.Errorf("cannot specify WriteQuorum greater than ReplicaCount")
	}

	if c.ReplicaRestoreDelay < 0 {
		return fmt.Errorf("cannot specify ReplicaRestoreDelay less than zero")
	}

	if err := c.validateMemberlistConfig(); err != nil {
		return err
	}

	if c.MemberCountQuorum < MinimumMemberCountQuorum {
		return fmt.Errorf("cannot specify MemberCountQuorum smaller than MinimumMemberCountQuorum")
	}

	if c.BindAddr == "" {
		return fmt.Errorf("bindAddr cannot be empty")
	}

	if c.BindPort == 0 {
		return fmt.Errorf("bindPort cannot be empty or zero")
	}

	// Check peers. If Peers slice contains node's itself, return an error.
	port := strconv.Itoa(c.MemberlistConfig.BindPort)
	this := net.JoinHostPort(c.MemberlistConfig.BindAddr, port)
	if slices.Contains(c.Peers, this) {
		return fmt.Errorf("cannot be peer with itself")
	}

	if err := c.Client.Validate(); err != nil {
		return fmt.Errorf("failed to validate client configuration: %w", err)
	}

	if err := c.DMaps.Validate(); err != nil {
		return err
	}

	if err := c.validateEvictionBudgets(c.DMaps.EvictionPolicy, c.DMaps.MaxInuse, c.DMaps.MaxKeys, "DMaps"); err != nil {
		return err
	}

	for name, custom := range c.DMaps.Custom {
		label := fmt.Sprintf("DMaps.Custom[%q]", name)
		if err := c.validateEvictionBudgets(custom.EvictionPolicy, custom.MaxInuse, custom.MaxKeys, label); err != nil {
			return err
		}
	}

	switch c.LogLevel {
	case LogLevelDebug, LogLevelWarn, LogLevelInfo, LogLevelError:
	default:
		return fmt.Errorf("invalid LogLevel: %s", c.LogLevel)
	}

	return nil
}

// validateEvictionBudgets guards against a configuration footgun where LRU eviction
// is enabled but the per-partition budget is too small for the storage engine to
// operate without runaway allocated-memory growth.
//
// The kvstore engine allocates memory in fixed-size, append-only tables. Each
// partition manages its own storage, so the effective per-partition budget is the
// node-wide limit divided by the partition count. When that budget is smaller than
// one storage table, or smaller than one key, eviction fires on nearly every write
// and tables accumulate as garbage faster than compaction can reclaim them, so the
// allocated memory grows far beyond the configured limit.
//
// PartitionCount is the conservative worst case: a single node owns every partition,
// while larger clusters give each node a bigger per-partition budget. The validation
// is skipped unless EvictionPolicy is LRU, since MaxInuse and MaxKeys have no effect
// otherwise.
func (c *Config) validateEvictionBudgets(policy EvictionPolicy, maxInuse, maxKeys int, label string) error {
	if policy != LRUEviction {
		return nil
	}

	partitionCount := int(c.PartitionCount)
	if partitionCount == 0 {
		partitionCount = DefaultPartitionCount
	}

	if maxInuse > 0 {
		tableSize := int(c.DMaps.Engine.TableSize())
		if perPartition := maxInuse / partitionCount; perPartition < tableSize {
			return fmt.Errorf(
				"%s: MaxInuse (%d bytes) split across PartitionCount (%d) gives %d bytes per partition, "+
					"smaller than the storage engine tableSize (%d bytes); increase MaxInuse to at least "+
					"PartitionCount*tableSize (%d bytes), reduce PartitionCount, or lower tableSize",
				label, maxInuse, partitionCount, perPartition, tableSize, partitionCount*tableSize,
			)
		}
	}

	if maxKeys > 0 && maxKeys < partitionCount {
		return fmt.Errorf(
			"%s: MaxKeys (%d) is smaller than PartitionCount (%d), so each partition gets a zero key budget "+
				"and evicts on every write; increase MaxKeys to at least PartitionCount (%d) or reduce PartitionCount",
			label, maxKeys, partitionCount, partitionCount,
		)
	}

	return nil
}

// Sanitize sets default values to empty configuration variables, if it's possible.
func (x *Config) Sanitize() error {
	if x.LogOutput == nil {
		x.LogOutput = os.Stderr
	}

	if x.LogLevel == "" {
		x.LogLevel = DefaultLogLevel
	}

	if x.LogVerbosity <= 0 {
		x.LogVerbosity = DefaultLogVerbosity
	}

	if x.Logger == nil {
		x.Logger = log.New(x.LogOutput, "", log.LstdFlags)
	} else {
		x.Logger.SetOutput(x.LogOutput)
	}

	if x.Hasher == nil {
		x.Hasher = hasher.NewDefaultHasher()
	}

	if x.BindAddr == "" {
		name, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to read hostname from kernel: %w", err)
		}
		x.BindAddr = name
	}
	// We currently don't support ephemeral port selection. Because it needs
	// improved flow control in server initialization stage.
	if x.BindPort == 0 {
		x.BindPort = DefaultPort
	}

	if x.LoadFactor == 0 {
		x.LoadFactor = DefaultLoadFactor
	}
	if x.PartitionCount == 0 {
		x.PartitionCount = DefaultPartitionCount
	}
	if x.ReplicaCount == 0 {
		x.ReplicaCount = MinimumReplicaCount
	}

	if x.ReadQuorum == 0 {
		x.ReadQuorum = DefaultReadQuorum
	}
	if x.WriteQuorum == 0 {
		x.WriteQuorum = DefaultWriteQuorum
	}

	if x.MemberCountQuorum == 0 {
		x.MemberCountQuorum = DefaultMemberCountQuorum
	}

	if x.MemberlistConfig == nil {
		m := memberlist.DefaultLocalConfig()
		// hostname is assigned to memberlist.BindAddr
		// memberlist.Name is assigned by olric.New
		m.BindPort = DefaultDiscoveryPort
		m.AdvertisePort = DefaultDiscoveryPort
		x.MemberlistConfig = m
	}

	if x.InitialSyncEmptyPartitionTimeout == 0 {
		x.InitialSyncEmptyPartitionTimeout = 15 * time.Second
	}
	if x.ReplicaRestoreDelay == 0 {
		x.ReplicaRestoreDelay = DefaultReplicaRestoreDelay
	}

	if x.BootstrapTimeout == 0 {
		x.BootstrapTimeout = DefaultBootstrapTimeout
	}
	if x.JoinRetryInterval == 0 {
		x.JoinRetryInterval = DefaultJoinRetryInterval
	}

	if x.MaxJoinAttempts == 0 {
		x.MaxJoinAttempts = DefaultMaxJoinAttempts
	}

	if x.RejoinInterval == 0 {
		x.RejoinInterval = DefaultRejoinInterval
	}

	if x.LeaveTimeout == 0 {
		x.LeaveTimeout = DefaultLeaveTimeout
	}

	if x.RoutingTablePushInterval == 0 {
		x.RoutingTablePushInterval = DefaultRoutingTablePushInterval
	}

	if x.TriggerBalancerInterval == 0 {
		x.TriggerBalancerInterval = DefaultTriggerBalancerInterval
	}

	if x.KeepAlivePeriod == 0 {
		x.KeepAlivePeriod = DefaultKeepAlivePeriod
	}

	if x.Client == nil {
		x.Client = NewClient()
	}

	if x.DMaps == nil {
		x.DMaps = &DMaps{}
	}

	if err := x.Client.Sanitize(); err != nil {
		return fmt.Errorf("failed to sanitize TCP client configuration: %w", err)
	}

	if err := x.DMaps.Sanitize(); err != nil {
		return fmt.Errorf("failed to sanitize DMap configuration: %w", err)
	}

	return nil
}

// New returns a Config with sane defaults. If you change a configuration parameter,
// please run Sanitize and Validate functions respectively.
//
// New takes an env parameter used by memberlist: local, lan and wan.
//
// local:
//
// DefaultLocalConfig works like DefaultConfig, however it returns a configuration
// that is optimized for a local loopback environments. The default configuration
// is still very conservative and errs on the side of caution.
//
// lan:
//
// DefaultLANConfig returns a sane set of configurations for Memberlist. It uses
// the hostname as the node name, and otherwise sets very conservative values
// that are sane for most LAN environments. The default configuration errs on
// the side of caution, choosing values that are optimized for higher convergence
// at the cost of higher bandwidth usage. Regardless, these values are a good
// starting point when getting started with memberlist.
//
// wan:
//
// DefaultWANConfig works like DefaultConfig, however it returns a configuration
// that is optimized for most WAN environments. The default configuration is still
// very conservative and errs on the side of caution.
func New(env string) *Config {
	c := &Config{
		BindAddr:                  "0.0.0.0",
		BindPort:                  DefaultPort,
		ReadRepair:                false,
		ReplicaCount:              1,
		WriteQuorum:               1,
		ReadQuorum:                1,
		MemberCountQuorum:         1,
		Peers:                     []string{},
		DMaps:                     &DMaps{},
		EnableProactiveSyncOnJoin: false,
	}

	m, err := NewMemberlistConfig(env)
	if err != nil {
		panic(fmt.Sprintf("unable to create a new memberlist config: %v", err))
	}
	// memberlist.Name will be assigned by olric.New
	m.BindPort = DefaultDiscoveryPort
	m.AdvertisePort = DefaultDiscoveryPort
	c.MemberlistConfig = m

	if err := c.Sanitize(); err != nil {
		panic(fmt.Sprintf("unable to sanitize Olric config: %v", err))
	}

	if err := c.Validate(); err != nil {
		panic(fmt.Sprintf("unable to validate Olric config: %v", err))
	}
	return c
}

// Interface guard
var _ IConfig = (*Config)(nil)
