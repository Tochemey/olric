#!/bin/bash

#
# Copyright 2018-2024 Burak Sezer
# Copyright 2025 Arsene Tochemey Gandote
#
#  Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

TMP=$(mktemp /tmp/olric-coverage-XXXXX.txt)

BUILD=$1
OUT=$2

set -e

# create coverage output
echo 'mode: atomic' > $OUT
for PKG in $(go list ./...| grep -v -E 'vendor'|grep -v -E 'hasher'|grep -v -E 'internal/bufpool'|
grep -v -E 'internal/flog'|grep -v -E 'serializer'|grep -v -E 'stats'|grep -v -E 'cmd'); do
  go test -v -covermode=atomic -coverprofile=$TMP $PKG
  tail -n +2 $TMP >> $OUT
done

