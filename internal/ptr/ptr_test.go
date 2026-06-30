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

package ptr

import (
	"testing"
)

func TestTo(t *testing.T) {
	v := 42
	ptr := To(v)
	if ptr == nil {
		t.Fatal("To() returned nil pointer")
	}
	if *ptr != v {
		t.Fatalf("To() = %v, want %v", *ptr, v)
	}
}

func TestDeref(t *testing.T) {
	v := 100
	def := 200
	ptr := &v
	if got := Deref(ptr, def); got != v {
		t.Fatalf("Deref(ptr, def) = %v, want %v", got, v)
	}
	if got := Deref(nil, def); got != def {
		t.Fatalf("Deref(nil, def) = %v, want %v", got, def)
	}
}

func TestEqual(t *testing.T) {
	a := 5
	b := 5
	c := 10
	var nilPtr *int
	if !Equal(&a, &b) {
		t.Error("Equal(&a, &b) = false, want true")
	}
	if Equal(&a, &c) {
		t.Error("Equal(&a, &c) = true, want false")
	}
	if !Equal(nilPtr, nilPtr) {
		t.Error("Equal(nil, nil) = false, want true")
	}
	if Equal(&a, nilPtr) {
		t.Error("Equal(&a, nil) = true, want false")
	}
	if Equal(nilPtr, &a) {
		t.Error("Equal(nil, &a) = true, want false")
	}
}
