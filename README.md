# Olric

[![build](https://img.shields.io/github/actions/workflow/status/Tochemey/olric/ci.yml?branch=main)](https://github.com/Tochemey/olric/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Tochemey/olric/branch/main/graph/badge.svg?token=C5Z0JE8SNj)](https://codecov.io/gh/Tochemey/olric)


This is forked version of the [main repository](https://github.com/tochemey/olric) with few bug fixes, refactoring, and it is only handles the embedded version.
Please use the original repo for any bugs or related questions.

## Modifications from the original library

* Support only embedded mode even though the majority of the code to run client/server is still there except the runner code.
* Remove Client/Server mode
* Renamed module name
* Upgrade **go version to 1.23.0**
* Refactor the readme to suit the behavior of this fork
* Fix some go routines leaks bugs
* Meta-information can be passed to the cluster member
* **TLS Support**

## Overview 

Olric is a distributed, in-memory key/value store and cache. It's designed from the ground up to be distributed, and it
can be
used both as an embedded Go library and as a language-independent service.

With Olric, you can instantly create a fast, scalable, shared pool of RAM across a cluster of computers.

Olric is implemented in [Go](https://go.dev/) and uses
the [Redis serialization protocol](https://redis.io/topics/protocol). So Olric has client implementations in all major
programming
languages.

Olric is highly scalable and available. Distributed applications can use it for distributed caching, clustering and
publish-subscribe messaging.

It is designed to scale out to hundreds of members and thousands of clients. When you add new members, they
automatically
discover the cluster and linearly increase the memory capacity. Olric offers simple scalability, partitioning (
sharding),
and re-balancing out-of-the-box. It does not require any extra coordination processes. With Olric, when you start
another
process to add more capacity, data and backups are automatically and evenly balanced.

See [Samples](#samples) sections to get started!

## At a glance

* Designed to share some transient, approximate, fast-changing data between servers,
* Uses Redis serialization protocol,
* Implements a distributed hash table,
* Provides a drop-in replacement for Redis Publish/Subscribe messaging system,
* Supports both programmatic and declarative configuration,
* Supports different eviction algorithms (including LRU and TTL),
* Highly available and horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP (indeed PA/EC) solution,
* Supports replication by default (with sync and async options),
* Quorum-based voting for replica control (Read/Write quorums),
* Supports atomic operations,
* Provides an iterator on distributed maps,
* Provides a plugin interface for service discovery daemons,
* Provides a locking primitive which inspired
  by [SETNX of Redis](https://redis.io/commands/setnx#design-pattern-locking-with-codesetnxcode),

## Possible Use Cases

Olric is an eventually consistent, unordered key/value data store. It supports various eviction mechanisms for
distributed caching implementations. Olric
also provides publish-subscribe messaging, data replication, failure detection and simple anti-entropy services.

It's good at distributed caching and publish/subscribe messaging.

## Table of Contents

* [Features](#features)
* [HowTo](#howto)
* [Cluster Events](#cluster-events)
* [Configuration](#configuration)
    * [Network Configuration](#network-configuration)
    * [Service discovery](#service-discovery)
    * [Timeouts](#timeouts)
* [Architecture](#architecture)
    * [Overview](#overview)
    * [Consistency and Replication Model](#consistency-and-replication-model)
        * [Last-write-wins conflict resolution](#last-write-wins-conflict-resolution)
        * [PACELC Theorem](#pacelc-theorem)
        * [Read-Repair on DMaps](#read-repair-on-dmaps)
        * [Quorum-based Replica Control](#quorum-based-replica-control)
        * [Simple Split-Brain Protection](#simple-split-brain-protection)
    * [Eviction](#eviction)
        * [Expire with TTL](#expire-with-ttl)
        * [Expire with MaxIdleDuration](#expire-with-maxidleduration)
        * [Expire with LRU](#expire-with-lru)
    * [Lock Implementation](#lock-implementation)
    * [Storage Engine](#storage-engine)
* [Samples](#samples)
* [Contributions](#contributions)
* [License](#license)
* [About the name](#about-the-name)

## Features

* Designed to share some transient, approximate, fast-changing data between servers,
* Accepts arbitrary types as value,
* Only in-memory,
* Uses Redis protocol,
* Compatible with existing Redis clients,
* Embeddable but can be used as a language-independent service with olricd,
* GC-friendly storage engine,
* O(1) running time for lookups,
* Supports atomic operations,
* Provides a lock implementation which can be used for non-critical purposes,
* Different eviction policies: LRU, MaxIdleDuration and Time-To-Live (TTL),
* Highly available,
* Horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP (indeed PA/EC) solution,
* Distributes load fairly among cluster members with
  a [consistent hash function](https://github.com/buraksezer/consistent),
* Supports replication by default (with sync and async options),
* Quorum-based voting for replica control,
* Thread-safe by default,
* Provides an iterator on distributed maps,
* Provides a plugin interface for service discovery daemons and cloud providers,
* Provides a locking primitive which inspired
  by [SETNX of Redis](https://redis.io/commands/setnx#design-pattern-locking-with-codesetnxcode),
* Provides a drop-in replacement of Redis' Publish-Subscribe messaging feature.

See [Architecture](#architecture) section to see details.

#### HowTo.

See [Samples](#samples) section to learn how to embed Olric into your existing Golang application.

## Cluster Events

Olric can send push cluster events to `cluster.events` channel. Available cluster events:

* node-join-event
* node-left-event
* fragment-migration-event
* fragment-received-even

If you want to receive these events, set `true` to `EnableClusterEventsChannel` and subscribe to `cluster.events`
channel.
The default is `false`.

See [events/cluster_events.go](events/cluster_events.go) file to get more information about events.

## Configuration

```go
import "github.com/tochemey/olric/config"
...
c := config.New("local")
```

The `New` function takes a parameter called `env`. It denotes the network environment and consumed
by [hashicorp/memberlist](https://github.com/hashicorp/memberlist).
Default configuration is good enough for distributed caching scenario. In order to see all configuration parameters,
please take a look at [this](https://godoc.org/github.com/tochemey/olric/config).

See [Sample Code](#samples) section for an introduction.

### Network Configuration

In an Olric instance, there are two different TCP servers. One for Olric, and the other one is for memberlist.
`BindAddr` is very
critical to deploy a healthy Olric node. There are different scenarios:

* You can freely set a domain name or IP address as `BindAddr` for both Olric and memberlist. Olric will resolve and use
  it to bind.
* You can freely set `localhost`, `127.0.0.1` or `::1` as `BindAddr` in development environment for both Olric and
  memberlist.
* You can freely set `0.0.0.0` as `BindAddr` for both Olric and memberlist. Olric will pick an IP address, if there is
  any.
* If you don't set `BindAddr`, hostname will be used, and it will be resolved to get a valid IP address.
* You can set a network interface by using `Config.Interface` and `Config.MemberlistInterface` fields. Olric will find
  an appropriate IP address for the given interfaces, if there is any.
* You can set both `BindAddr` and interface parameters. In this case Olric will ensure that `BindAddr` is available on
  the given interface.

You should know that Olric needs a single and stable IP address to function properly. If you don't know the IP address
of the host at the deployment time,
you can set `BindAddr` as `0.0.0.0`. Olric will very likely to find an IP address for you.

### Service Discovery

Olric provides a service discovery interface which can be used to implement plugins.

### Timeouts

Olric nodes supports setting `KeepAlivePeriod` on TCP sockets.

**Server-side:**

##### config.KeepAlivePeriod

KeepAlivePeriod denotes whether the operating system should send keep-alive messages on the connection.

**Client-side:**

##### config.DialTimeout

Timeout for TCP dial. The timeout includes name resolution, if required. When using TCP, and the host in the address
parameter resolves to multiple IP addresses, the timeout is spread over each consecutive dial, such that each is
given an appropriate fraction of the time to connect.

##### config.ReadTimeout

Timeout for socket reads. If reached, commands will fail with a timeout instead of blocking. Use value -1 for no
timeout and 0 for default. The default is config.DefaultReadTimeout

##### config.WriteTimeout

Timeout for socket writes. If reached, commands will fail with a timeout instead of blocking. The default is
config.DefaultWriteTimeout

## Architecture

### Overview

Olric uses:

* [hashicorp/memberlist](https://github.com/hashicorp/memberlist) for cluster membership and failure detection,
* [buraksezer/consistent](https://github.com/buraksezer/consistent) for consistent hashing and load balancing,
* [Redis Serialization Protocol](https://github.com/tidwall/redcon) for communication.

Olric distributes data among partitions. Every partition is being owned by a cluster member and may have one or more
backups for redundancy.
When you read or write a DMap entry, you transparently talk to the partition owner. Each request hits the most
up-to-date version of a
particular data entry in a stable cluster.

In order to find the partition which the key belongs to, Olric hashes the key and mod it with the number of partitions:

```
partID = MOD(hash result, partition count)
```

The partitions are being distributed among cluster members by using a consistent hashing algorithm. In order to get
details, please see [buraksezer/consistent](https://github.com/buraksezer/consistent).

When a new cluster is created, one of the instances is elected as the **cluster coordinator**. It manages the partition
table:

* When a node joins or leaves, it distributes the partitions and their backups among the members again,
* Removes empty previous owners from the partition owners list,
* Pushes the new partition table to all the members,
* Pushes the partition table to the cluster periodically.

Members propagate their birthdate(POSIX time in nanoseconds) to the cluster. The coordinator is the oldest member in the
cluster.
If the coordinator leaves the cluster, the second oldest member gets elected as the coordinator.

Olric has a component called **rebalancer** which is responsible for keeping underlying data structures consistent:

* Works on every node,
* When a node joins or leaves, the cluster coordinator pushes the new partition table. Then, the **rebalancer** runs
  immediately and moves the partitions and backups to their new hosts,
* Merges fragmented partitions.

Partitions have a concept called **owners list**. When a node joins or leaves the cluster, a new primary owner may be
assigned by the
coordinator. At any time, a partition may have one or more partition owners. If a partition has two or more owners, this
is called **fragmented partition**.
The last added owner is called **primary owner**. Write operation is only done by the primary owner. The previous owners
are only used for read and delete.

When you read a key, the primary owner tries to find the key on itself, first. Then, queries the previous owners and
backups, respectively.
The delete operation works the same way.

The data(distributed map objects) in the fragmented partition is moved slowly to the primary owner by the **rebalancer**. Until the move is done,
the data remains available on the previous owners. The DMap methods use this list to query data on the cluster.

*Please note that, 'multiple partition owners' is an undesirable situation and the **rebalancer** component is designed
to fix that in a short time.*

### Consistency and Replication Model

**Olric is an AP product** in the context of [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem), which employs the
combination of primary-copy
and [optimistic replication](https://en.wikipedia.org/wiki/Optimistic_replication) techniques. With optimistic
replication, when the partition owner
receives a write or delete operation for a key, applies it locally, and propagates it to the backup owners.

This technique enables Olric clusters to offer high throughput. However, due to temporary situations in the system, such
as network
failure, backup owners can miss some updates and diverge from the primary owner. If a partition owner crashes while
there is an
inconsistency between itself and the backups, strong consistency of the data can be lost.

Two types of backup replication are available: **sync** and **async**. Both types are still implementations of the
optimistic replication
model.

* **sync**: Blocks until write/delete operation is applied by backup owners.
* **async**: Just fire & forget.

#### Last-write-wins conflict resolution

Every time a piece of data is written to Olric, a timestamp is attached by the client. Then, when Olric has to deal with
conflict data in the case
of network partitioning, it simply chooses the data with the most recent timestamp. This called LWW conflict resolution
policy.

#### PACELC Theorem

From Wikipedia:

> In theoretical computer science, the [PACELC theorem](https://en.wikipedia.org/wiki/PACELC_theorem) is an extension to
> the [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem). It states that in case of network partitioning (P) in a
> distributed computer system, one has to choose between availability (A) and consistency (C) (as per the CAP theorem),
> but else (E), even when the system is
> running normally in the absence of partitions, one has to choose between latency (L) and consistency (C).

In the context of PACELC theorem, Olric is a **PA/EC** product. It means that Olric is considered to be **consistent**
data store if the network is stable.
Because the key space is divided between partitions and every partition is controlled by its primary owner. All
operations on DMaps are redirected to the
partition owner.

In the case of network partitioning, Olric chooses **availability** over consistency. So that you can still access some
parts of the cluster when the network is unreliable,
but the cluster may return inconsistent results.

Olric implements read-repair and quorum based voting system to deal with inconsistencies in the DMaps.

Readings on PACELC theorem:

* [Please stop calling databases CP or AP](https://martin.kleppmann.com/2015/05/11/please-stop-calling-databases-cp-or-ap.html)
* [Problems with CAP, and Yahoo’s little known NoSQL system](https://dbmsmusings.blogspot.com/2010/04/problems-with-cap-and-yahoos-little.html)
* [A Critique of the CAP Theorem](https://arxiv.org/abs/1509.05393)
* [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html)

#### Read-Repair on DMaps

Read repair is a feature that allows for inconsistent data to be fixed at query time. Olric tracks every write operation
with a timestamp value and assumes
that the latest write operation is the valid one. When you want to access a key/value pair, the partition owner
retrieves all available copies for that pair
and compares the timestamp values. The latest one is the winner. If there is some outdated version of the requested
pair, the primary owner propagates the latest
version of the pair.

Read-repair is disabled by default for the sake of performance. If you have a use case that requires a more strict
consistency control than a distributed caching
scenario, you can enable read-repair via the configuration.

#### Quorum-based replica control

Olric implements Read/Write quorum to keep the data in a consistent state. When you start a write operation on the
cluster and write quorum (W) is 2,
the partition owner tries to write the given key/value pair on its own data storage and on the replica nodes. If the
number of successful write operations
is below W, the primary owner returns `ErrWriteQuorum`. The read flow is the same: if you have R=2 and the owner only
access one of the replicas,
it returns `ErrReadQuorum`.

#### Simple Split-Brain Protection

Olric implements a technique called *majority quorum* to manage split-brain conditions. If a network partitioning
occurs, and some members
lost the connection to rest of the cluster, they immediately stops functioning and return an error to incoming requests.
This behaviour is controlled by
`MemberCountQuorum` parameter. It's default `1`.

When the network healed, the stopped nodes joins again the cluster and fragmented partitions is merged by their primary
owners in accordance with
*LWW policy*. Olric also implements an *ownership report* mechanism to fix inconsistencies in partition distribution
after a partitioning event.

### Eviction

Olric supports different policies to evict keys from distributed maps.

#### Expire with TTL

Olric implements TTL eviction policy. It shares the same algorithm
with [Redis](https://redis.io/commands/expire#appendix-redis-expires):

> Periodically Redis tests a few keys at random among keys with an expire set. All the keys that are already expired are
> deleted from the keyspace.
>
> Specifically this is what Redis does 10 times per second:
>
> * Test 20 random keys from the set of keys with an associated expire.
> * Delete all the keys found expired.
> * If more than 25% of keys were expired, start again from step 1.
>
> This is a trivial probabilistic algorithm, basically the assumption is that our sample is representative of the whole
> key space, and we continue to expire until the percentage of keys that are likely to be expired is under 25%

When a client tries to access a key, Olric returns `ErrKeyNotFound` if the key is found to be timed out. A background
task evicts keys with the algorithm described above.

#### Expire with MaxIdleDuration

Maximum time for each entry to stay idle in the DMap. It limits the lifetime of the entries relative to the time of the
last read
or write access performed on them. The entries whose idle period exceeds this limit are expired and evicted
automatically.
An entry is idle if no Get, Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration feature varies by
preferred deployment method.

#### Expire with LRU

Olric implements LRU eviction method on DMaps. Approximated LRU algorithm is borrowed from Redis. The Redis authors
proposes the following algorithm:

> It is important to understand that the eviction process works like this:
>
> * A client runs a new command, resulting in more data added.
> * Redis checks the memory usage, and if it is greater than the maxmemory limit , it evicts keys according to the
    policy.
> * A new command is executed, and so forth.
>
> So we continuously cross the boundaries of the memory limit, by going over it, and then by evicting keys to return
> back under the limits.
>
> If a command results in a lot of memory being used (like a big set intersection stored into a new key) for some time
> the memory
> limit can be surpassed by a noticeable amount.
>
> **Approximated LRU algorithm**
>
> Redis LRU algorithm is not an exact implementation. This means that Redis is not able to pick the best candidate for
> eviction,
> that is, the access that was accessed the most in the past. Instead it will try to run an approximation of the LRU
> algorithm,
> by sampling a small number of keys, and evicting the one that is the best (with the oldest access time) among the
> sampled keys.

Olric tracks access time for every DMap instance. Then it picks and sorts some configurable amount of keys to select
keys for eviction.
Every node runs this algorithm independently. The access log is moved along with the partition when a network partition
is occured.

#### Configuration of eviction mechanisms

For embedded-member deployment scenario, please take a look
at [config#CacheConfig](https://godoc.org/github.com/tochemey/olric/config#CacheConfig)
and [config#DMapCacheConfig](https://godoc.org/github.com/tochemey/olric/config#DMapCacheConfig) for the
configuration.

### Lock Implementation

The DMap implementation is already thread-safe to meet your thread safety requirements. When you want to have more
control on the
concurrency, you can use **LockWithTimeout** and **Lock** methods. Olric borrows the locking algorithm from Redis. Redis
authors propose
the following algorithm:

> The command <SET resource-name anystring NX EX max-lock-time> is a simple way to implement a locking system with
> Redis.
>
> A client can acquire the lock if the above command returns OK (or retry after some time if the command returns Nil),
> and remove the lock just using DEL.
>
> The lock will be auto-released after the expire time is reached.
>
> It is possible to make this system more robust modifying the unlock schema as follows:
>
> Instead of setting a fixed string, set a non-guessable large random string, called token.
> Instead of releasing the lock with DEL, send a script that only removes the key if the value matches.
> This avoids that a client will try to release the lock after the expire time deleting the key created by another
> client that acquired the lock later.

Equivalent of`SETNX` command in Olric is `PutIf(key, value, IfNotFound)`. Lock and LockWithTimeout commands are properly
implements
the algorithm which is proposed above.

You should know that this implementation is subject to the clustering algorithm. So there is no guarantee about
reliability in the case of network partitioning. I recommend the lock implementation to be used for
efficiency purposes in general, instead of correctness.

**Important note about consistency:**

You should know that Olric is a PA/EC (see [Consistency and Replication Model](#consistency-and-replication-model))
product. So if your network is stable, all the operations on key/value
pairs are performed by a single cluster member. It means that you can be sure about the consistency when the cluster is
stable. It's important to know that computer networks fail
occasionally, processes crash and random GC pauses may happen. Many factors can lead a network partitioning. If you
cannot tolerate losing strong consistency under network partitioning,
you need to use a different tool for locking.

See [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html)
and [Jepsen Analysis on Hazelcast 3.8.3](https://hazelcast.com/blog/jepsen-analysis-hazelcast-3-8-3/) for more insight
on this topic.

### Storage Engine

Olric implements a GC-friendly storage engine to store large amounts of data on RAM. Basically, it applies an
append-only log file approach with indexes.
Olric inserts key/value pairs into pre-allocated byte slices (table in Olric terminology) and indexes that memory region
by using Golang's built-in map.
The data type of this map is `map[uint64]uint64`. When a pre-allocated byte slice is full Olric allocates a new one and
continues inserting the new data into it.
This design greatly reduces the write latency.

When you want to read a key/value pair from the Olric cluster, it scans the related DMap fragment by iterating over the
indexes(implemented by the built-in map).
The number of allocated byte slices should be small. So Olric would find the key immediately but technically, the read
performance depends on the number of keys in the fragment.
The effect of this design on the read performance is negligible.

The size of the pre-allocated byte slices is configurable.

## Samples

In this section, you can find code snippets for various scenarios.

### Embedded-member scenario

#### Distributed map

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tochemey/olric"
	"github.com/tochemey/olric/config"
)

func main() {
	// Sample for Olric v0.5.x

	// Deployment scenario: embedded-member
	// This creates a single-node Olric cluster. It's good enough for experimenting.

	// config.New returns a new config.Config with sane defaults. Available values for env:
	// local, lan, wan
	c := config.New("local")

	// Callback function. It's called when this node is ready to accept connections.
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		defer cancel()
		log.Println("[INFO] Olric is ready to accept connections")
	}

	// Create a new Olric instance.
	db, err := olric.New(c)
	if err != nil {
		log.Fatalf("Failed to create Olric instance: %v", err)
	}

	// Start the instance. It will form a single-node cluster.
	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatalf("olric.Start returned an error: %v", err)
		}
	}()

	<-ctx.Done()

	// In embedded-member scenario, you can use the EmbeddedClient. It implements
	// the Client interface.
	e := db.NewEmbeddedClient()

	dm, err := e.NewDMap("bucket-of-arbitrary-items")
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	// Magic starts here!
	fmt.Println("##")
	fmt.Println("Simple Put/Get on a DMap instance:")
	err = dm.Put(ctx, "my-key", "Olric Rocks!")
	if err != nil {
		log.Fatalf("Failed to call Put: %v", err)
	}

	gr, err := dm.Get(ctx, "my-key")
	if err != nil {
		log.Fatalf("Failed to call Get: %v", err)
	}

	// Olric uses the Redis serialization format.
	value, err := gr.String()
	if err != nil {
		log.Fatalf("Failed to read Get response: %v", err)
	}

	fmt.Println("Response for my-key:", value)
	fmt.Println("##")

	// Don't forget the call Shutdown when you want to leave the cluster.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.Shutdown(ctx)
	if err != nil {
		log.Printf("Failed to shutdown Olric: %v", err)
	}
}
```

#### Publish-Subscribe

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tochemey/olric"
	"github.com/tochemey/olric/config"
)

func main() {
	// Sample for Olric v0.5.x

	// Deployment scenario: embedded-member
	// This creates a single-node Olric cluster. It's good enough for experimenting.

	// config.New returns a new config.Config with sane defaults. Available values for env:
	// local, lan, wan
	c := config.New("local")

	// Callback function. It's called when this node is ready to accept connections.
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		defer cancel()
		log.Println("[INFO] Olric is ready to accept connections")
	}

	// Create a new Olric instance.
	db, err := olric.New(c)
	if err != nil {
		log.Fatalf("Failed to create Olric instance: %v", err)
	}

	// Start the instance. It will form a single-node cluster.
	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatalf("olric.Start returned an error: %v", err)
		}
	}()

	<-ctx.Done()

	// In embedded-member scenario, you can use the EmbeddedClient. It implements
	// the Client interface.
	e := db.NewEmbeddedClient()

	ps, err := e.NewPubSub()
	if err != nil {
		log.Fatalf("olric.NewPubSub returned an error: %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	// Olric implements a drop-in replacement of Redis Publish-Subscribe messaging
	// system. PubSub client is just a thin layer around go-redis/redis.
	rps := ps.Subscribe(ctx, "my-channel")

	// Get a message to read messages from my-channel
	msg := rps.Channel()

	go func() {
		// Publish a message here.
		_, err := ps.Publish(ctx, "my-channel", "Olric Rocks!")
		if err != nil {
			log.Fatalf("PubSub.Publish returned an error: %v", err)
		}
	}()

	// Consume messages
	rm := <-msg

	fmt.Printf("Received message: \"%s\" from \"%s\"", rm.Channel, rm.Payload)

	// Don't forget the call Shutdown when you want to leave the cluster.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = e.Close(ctx)
	if err != nil {
		log.Printf("Failed to close EmbeddedClient: %v", err)
	}
}
```

### SCAN on DMaps

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tochemey/olric"
	"github.com/tochemey/olric/config"
)

func main() {
	// Sample for Olric v0.5.x

	// Deployment scenario: embedded-member
	// This creates a single-node Olric cluster. It's good enough for experimenting.

	// config.New returns a new config.Config with sane defaults. Available values for env:
	// local, lan, wan
	c := config.New("local")

	// Callback function. It's called when this node is ready to accept connections.
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		defer cancel()
		log.Println("[INFO] Olric is ready to accept connections")
	}

	// Create a new Olric instance.
	db, err := olric.New(c)
	if err != nil {
		log.Fatalf("Failed to create Olric instance: %v", err)
	}

	// Start the instance. It will form a single-node cluster.
	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatalf("olric.Start returned an error: %v", err)
		}
	}()

	<-ctx.Done()

	// In embedded-member scenario, you can use the EmbeddedClient. It implements
	// the Client interface.
	e := db.NewEmbeddedClient()

	dm, err := e.NewDMap("bucket-of-arbitrary-items")
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	// Magic starts here!
	fmt.Println("##")
	fmt.Println("Insert 10 keys")
	var key string
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			key = fmt.Sprintf("even:%d", i)
		} else {
			key = fmt.Sprintf("odd:%d", i)
		}
		err = dm.Put(ctx, key, nil)
		if err != nil {
			log.Fatalf("Failed to call Put: %v", err)
		}
	}

	i, err := dm.Scan(ctx)
	if err != nil {
		log.Fatalf("Failed to call Scan: %v", err)
	}

	fmt.Println("Iterate over all the keys")
	for i.Next() {
		fmt.Println(">> Key", i.Key())
	}

	i.Close()

	i, err = dm.Scan(ctx, olric.Match("^even:"))
	if err != nil {
		log.Fatalf("Failed to call Scan: %v", err)
	}

	fmt.Println("\n\nScan with regex: ^even:")
	for i.Next() {
		fmt.Println(">> Key", i.Key())
	}

	i.Close()

	// Don't forget the call Shutdown when you want to leave the cluster.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.Shutdown(ctx)
	if err != nil {
		log.Printf("Failed to shutdown Olric: %v", err)
	}
}
```

#### Publish-Subscribe

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tochemey/olric"
)

func main() {
	// Sample for Olric v0.5.x

	// Deployment scenario: client-server

	// NewClusterClient takes a list of the nodes. This list may only contain a
	// load balancer address. Please note that Olric nodes will calculate the partition owner
	// and proxy the incoming requests.
	c, err := olric.NewClusterClient([]string{"localhost:3320"})
	if err != nil {
		log.Fatalf("olric.NewClusterClient returned an error: %v", err)
	}

	// In client-server scenario, you can use the ClusterClient. It implements
	// the Client interface.
	ps, err := c.NewPubSub()
	if err != nil {
		log.Fatalf("olric.NewPubSub returned an error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Olric implements a drop-in replacement of Redis Publish-Subscribe messaging
	// system. PubSub client is just a thin layer around go-redis/redis.
	rps := ps.Subscribe(ctx, "my-channel")

	// Get a message to read messages from my-channel
	msg := rps.Channel()

	go func() {
		// Publish a message here.
		_, err := ps.Publish(ctx, "my-channel", "Olric Rocks!")
		if err != nil {
			log.Fatalf("PubSub.Publish returned an error: %v", err)
		}
	}()

	// Consume messages
	rm := <-msg

	fmt.Printf("Received message: \"%s\" from \"%s\"", rm.Channel, rm.Payload)

	// Don't forget the call Shutdown when you want to leave the cluster.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = c.Close(ctx)
	if err != nil {
		log.Printf("Failed to close ClusterClient: %v", err)
	}
}

```

## Contributions

Please don't hesitate to fork the project and send a pull request or just e-mail me to ask questions and share ideas.

## License

The Apache License, Version 2.0 - see LICENSE for more details.

## About the name

The inner voice of Turgut Özben who is the main character
of [Oğuz Atay's masterpiece -The Disconnected-](https://www.themodernnovel.org/asia/other-asia/turkey/oguz-atay/the-disconnected/).
