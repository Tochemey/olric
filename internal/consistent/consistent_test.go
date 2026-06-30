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

package consistent

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"testing"
)

func newConfig() Config {
	return Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
}

type testMember string

func (tm testMember) String() string {
	return string(tm)
}

type hasher struct{}

func (hs hasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func TestConsistentAdd(t *testing.T) {
	cfg := newConfig()
	c := New(nil, cfg)
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members[member.String()] = struct{}{}
		c.Add(member)
	}
	for member := range members {
		found := false
		for _, mem := range c.GetMembers() {
			if member == mem.String() {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s could not be found", member)
		}
	}
}

func TestConsistentRemove(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := New(members, cfg)
	if len(c.GetMembers()) != len(members) {
		t.Fatalf("inserted member count is different")
	}
	for _, member := range members {
		c.Remove(member.String())
	}
	if len(c.GetMembers()) != 0 {
		t.Fatalf("member count should be zero")
	}
}

func TestConsistentLoad(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()

	t.Run("Average load should be greater than the member's load", func(t *testing.T) {
		c := New(members, cfg)
		if len(c.GetMembers()) != len(members) {
			t.Fatalf("inserted member count is different")
		}
		maxLoad := c.AverageLoad()
		for member, load := range c.LoadDistribution() {
			if load > maxLoad {
				t.Fatalf("%s exceeds max load. Its load: %f, max load: %f", member, load, maxLoad)
			}
		}
	})

	t.Run("Average load should equal to zero if there are no members", func(t *testing.T) {
		c := New(nil, cfg)
		if c.AverageLoad() != 0 {
			t.Fatalf("AverageLoad should equal to zero")
		}
	})
}

func TestConsistentDoesNotPanicWhenMembersExceedPartitions(t *testing.T) {
	cfg := Config{
		PartitionCount:    5,
		ReplicationFactor: 1,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := New([]Member{
		testMember("node1"),
		testMember("node2"),
		testMember("node3"),
	}, cfg)

	// Adding more members should not panic even when partition count is smaller than member count.
	if err := func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("add panicked: %v", rec)
			}
		}()
		for i := 4; i <= 7; i++ {
			c.Add(testMember(fmt.Sprintf("node%d", i)))
		}
		return nil
	}(); err != nil {
		t.Fatal(err)
	}

	expectedAvg := math.Ceil((float64(cfg.PartitionCount) / float64(len(c.GetMembers()))) * cfg.Load)
	if got := c.AverageLoad(); got != expectedAvg {
		t.Fatalf("average load mismatch. expected: %v, got: %v", expectedAvg, got)
	}

	var total float64
	for member, load := range c.LoadDistribution() {
		if load > expectedAvg {
			t.Fatalf("%s exceeds max load. load: %f, max: %f", member, load, expectedAvg)
		}
		total += load
	}
	if math.Abs(total-float64(cfg.PartitionCount)) > 1e-9 {
		t.Fatalf("total partition load mismatch. expected: %d, got: %f", cfg.PartitionCount, total)
	}
}

func TestConsistentMoreMembersThanPartitions(t *testing.T) {
	cfg := Config{
		PartitionCount:    3,
		ReplicationFactor: 1,
		Load:              1.25,
		Hasher:            hasher{},
	}
	members := []Member{
		testMember("node1"),
		testMember("node2"),
		testMember("node3"),
		testMember("node4"),
		testMember("node5"),
	}

	// Should not panic when initializing with more members than partitions.
	c := New(members, cfg)

	expectedAvg := math.Ceil((float64(cfg.PartitionCount) / float64(len(c.GetMembers()))) * cfg.Load)
	if got := c.AverageLoad(); got != expectedAvg {
		t.Fatalf("average load mismatch. expected: %v, got: %v", expectedAvg, got)
	}

	loads := c.LoadDistribution()
	if len(loads) != cfg.PartitionCount {
		t.Fatalf("expected %d members to hold partitions, got %d", cfg.PartitionCount, len(loads))
	}
	var total float64
	for member, load := range loads {
		if load > expectedAvg {
			t.Fatalf("%s exceeds max load. load: %f, max: %f", member, load, expectedAvg)
		}
		total += load
	}
	if math.Abs(total-float64(cfg.PartitionCount)) > 1e-9 {
		t.Fatalf("total partition load mismatch. expected: %d, got: %f", cfg.PartitionCount, total)
	}
}

func TestConsistentLocateKey(t *testing.T) {
	cfg := newConfig()
	c := New(nil, cfg)
	key := []byte("Olric")
	res := c.LocateKey(key)
	if res != nil {
		t.Fatalf("This should be nil: %v", res)
	}
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members[member.String()] = struct{}{}
		c.Add(member)
	}
	res = c.LocateKey(key)
	if res == nil {
		t.Fatalf("This shouldn't be nil: %v", res)
	}
}

func TestConsistentInsufficientMemberCount(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := New(members, cfg)
	key := []byte("Olric")
	_, err := c.GetClosestN(key, 30)
	if err != ErrInsufficientMemberCount {
		t.Fatalf("Expected ErrInsufficientMemberCount(%v), Got: %v", ErrInsufficientMemberCount, err)
	}
}

func TestConsistentClosestMembers(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := New(members, cfg)
	key := []byte("Olric")
	closestn, err := c.GetClosestN(key, 2)
	if err != nil {
		t.Fatalf("Expected nil, Got: %v", err)
	}
	if len(closestn) != 2 {
		t.Fatalf("Expected closest member count is 2. Got: %d", len(closestn))
	}
	partID := c.FindPartitionID(key)
	owner := c.GetPartitionOwner(partID)
	for i, cl := range closestn {
		if i != 0 && cl.String() == owner.String() {
			t.Fatalf("Backup is equal the partition owner: %s", owner.String())
		}
	}
}

func BenchmarkAddRemove(b *testing.B) {
	cfg := newConfig()
	c := New(nil, cfg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := testMember("node" + strconv.Itoa(i))
		c.Add(member)
		c.Remove(member.String())
	}
}

func BenchmarkLocateKey(b *testing.B) {
	cfg := newConfig()
	c := New(nil, cfg)
	c.Add(testMember("node1"))
	c.Add(testMember("node2"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + strconv.Itoa(i))
		c.LocateKey(key)
	}
}

func BenchmarkGetClosestN(b *testing.B) {
	cfg := newConfig()
	c := New(nil, cfg)
	for i := 0; i < 10; i++ {
		c.Add(testMember(fmt.Sprintf("node%d", i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + strconv.Itoa(i))
		_, _ = c.GetClosestN(key, 3)
	}
}
