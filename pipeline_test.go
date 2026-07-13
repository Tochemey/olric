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

package olric

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/testutil"
)

func TestDMapPipeline_Put(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FuturePut)
	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	for i := 0; i < 100; i++ {
		fp, err := pipe.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
		futures[i] = fp
	}
	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for _, fp := range futures {
		require.NoError(t, fp.Result())
	}

	for i := 0; i < 100; i++ {
		key := testutil.ToKey(i)
		gr, err := dm.Get(ctx, key)
		require.NoError(t, err)

		value, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), value)
	}
}

func TestDMapPipeline_Get(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
	}

	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	futures := make(map[int]*FutureGet)
	for i := 0; i < 100; i++ {
		fg := pipe.Get(ctx, testutil.ToKey(i))
		futures[i] = fg
	}

	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for i, fg := range futures {
		gr, err := fg.Result()
		require.NoError(t, err)

		value, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), value)
	}
}

func TestFutureGet_Result_NilPipelineValue(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.get", "mydmap", "missing", "RW")
	cmd.SetVal(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	future := &FutureGet{
		dp: &DMapPipeline{
			result: map[uint64][]redis.Cmder{0: {cmd}},
		},
		ctx:       ctx,
		closedCtx: context.Background(),
	}

	res, err := future.Result()
	require.Nil(t, res)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMapPipeline_Delete(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
	}

	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	futures := make(map[int]*FutureDelete)
	for i := 0; i < 100; i++ {
		fd := pipe.Delete(ctx, testutil.ToKey(i))
		futures[i] = fd
	}

	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for _, fd := range futures {
		num, err := fd.Result()
		require.NoError(t, err)
		require.Equal(t, 1, num)
	}
}

func TestDMapPipeline_Expire(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
	}

	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	futures := make(map[int]*FutureExpire)
	for i := 0; i < 100; i++ {
		fd, err := pipe.Expire(ctx, testutil.ToKey(i), time.Hour)
		require.NoError(t, err)
		futures[i] = fd
	}

	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for _, fd := range futures {
		err := fd.Result()
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		gr, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.NotEqual(t, int64(0), gr.TTL())
	}
}

func TestDMapPipeline_Incr(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FutureIncr)
	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	for i := 0; i < 100; i++ {
		fi, err := pipe.Incr(ctx, "mykey", 1)
		require.NoError(t, err)
		futures[i] = fi
	}
	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for i, fp := range futures {
		num, err := fp.Result()
		require.NoError(t, err)
		require.Equal(t, i+1, num)
	}
}

func TestDMapPipeline_Decr(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FutureDecr)
	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	for i := 0; i < 100; i++ {
		fi, err := pipe.Decr(ctx, "mykey", 1)
		require.NoError(t, err)
		futures[i] = fi
	}
	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for i, fp := range futures {
		num, err := fp.Result()
		require.NoError(t, err)
		require.Equal(t, -1*(i+1), num)
	}
}

func TestDMapPipeline_GetPut(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FutureGetPut)
	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	for i := 0; i < 100; i++ {
		fi, err := pipe.GetPut(ctx, "key", testutil.ToVal(i))
		require.NoError(t, err)
		futures[i] = fi
	}
	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for _, fp := range futures {
		_, err := fp.Result()
		require.NoError(t, err)
	}
}

func TestFutureGetPut_Result_NilPipelineValue(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.getput", "mydmap", "key", "value", "RW")
	cmd.SetVal(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	future := &FutureGetPut{
		dp: &DMapPipeline{
			result: map[uint64][]redis.Cmder{0: {cmd}},
		},
		ctx:       ctx,
		closedCtx: context.Background(),
	}

	res, err := future.Result()
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestDMapPipeline_IncrByFloat(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FutureIncrByFloat)
	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	for i := 0; i < 100; i++ {
		fi, err := pipe.IncrByFloat(ctx, "mykey", 1.2)
		require.NoError(t, err)
		futures[i] = fi
	}
	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for _, fp := range futures {
		_, err := fp.Result()
		require.NoError(t, err)
	}
}

func TestDMapPipeline_Discard(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FuturePut)
	pipe, err := dm.Pipeline()
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		fp, err := pipe.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
		futures[i] = fp
	}

	// Discard all pipelined DM.PUT requests.
	err = pipe.Discard()
	require.NoError(t, err)

	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		key := testutil.ToKey(i)
		_, err := dm.Get(ctx, key)
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMapPipeline_Close(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FuturePut)
	pipe, err := dm.Pipeline()
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		fp, err := pipe.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
		futures[i] = fp
	}

	pipe.Close()

	err = pipe.Exec(ctx)
	require.ErrorIs(t, err, ErrPipelineClosed)
}

func TestDMapPipeline_ErrNotReady(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	t.Run("Put", func(t *testing.T) {
		fp, err := pipe.Put(ctx, "key", "value")
		require.NoError(t, err)
		require.ErrorIs(t, ErrNotReady, fp.Result())
	})

	t.Run("Get", func(t *testing.T) {
		fp := pipe.Get(ctx, "key")
		_, err := fp.Result()
		require.ErrorIs(t, ErrNotReady, err)
	})

	t.Run("Delete", func(t *testing.T) {
		fp := pipe.Delete(ctx, "key")
		_, err := fp.Result()
		require.ErrorIs(t, ErrNotReady, err)
	})

	t.Run("Expire", func(t *testing.T) {
		fp, err := pipe.Expire(ctx, "key", time.Second)
		require.NoError(t, err)
		err = fp.Result()
		require.ErrorIs(t, ErrNotReady, err)
	})

	t.Run("Incr", func(t *testing.T) {
		fp, err := pipe.Incr(ctx, "key", 1)
		require.NoError(t, err)
		_, err = fp.Result()
		require.ErrorIs(t, ErrNotReady, err)
	})

	t.Run("Decr", func(t *testing.T) {
		fp, err := pipe.Decr(ctx, "key", 1)
		require.NoError(t, err)
		_, err = fp.Result()
		require.ErrorIs(t, ErrNotReady, err)
	})

	t.Run("GetPut", func(t *testing.T) {
		fp, err := pipe.GetPut(ctx, "key", "value")
		require.NoError(t, err)
		_, err = fp.Result()
		require.ErrorIs(t, ErrNotReady, err)
	})

	t.Run("IncrByFloat", func(t *testing.T) {
		fp, err := pipe.IncrByFloat(ctx, "key", 1)
		require.NoError(t, err)
		_, err = fp.Result()
		require.ErrorIs(t, ErrNotReady, err)
	})
}

func TestDMapPipeline_EmbeddedClient(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c := db.NewEmbeddedClient()
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FuturePut)
	pipe, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipe.Close()

	for i := 0; i < 100; i++ {
		fp, err := pipe.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
		futures[i] = fp
	}
	err = pipe.Exec(ctx)
	require.NoError(t, err)

	for _, fp := range futures {
		require.NoError(t, fp.Result())
	}

	for i := 0; i < 100; i++ {
		key := testutil.ToKey(i)
		gr, err := dm.Get(ctx, key)
		require.NoError(t, err)

		value, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), value)
	}
}

func TestDMapPipeline_setOrGetClusterClient(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c := db.NewEmbeddedClient()
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	pipeOne, err := dm.Pipeline()
	require.NoError(t, err)
	defer pipeOne.Close()

	require.NotNil(t, db.clusterClient)
}

func TestPipelineStringValue_UnexpectedCmdType(t *testing.T) {
	// A *redis.StringCmd is not the expected *redis.Cmd type.
	cmd := redis.NewStringCmd(context.Background(), "dm.get", "mydmap", "key")
	_, _, err := pipelineStringValue(cmd)
	require.ErrorIs(t, err, ErrUnexpectedPipelineCmdType)
}

func TestPipelineStringValue_UnexpectedValueType(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.get", "mydmap", "key")
	cmd.SetVal(int64(123)) // not a string
	_, _, err := pipelineStringValue(cmd)
	require.ErrorIs(t, err, ErrUnexpectedPipelineValueType)
}

func TestPipelineStringValue_OK(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.get", "mydmap", "key")
	cmd.SetVal("hello")
	value, nilValue, err := pipelineStringValue(cmd)
	require.NoError(t, err)
	require.False(t, nilValue)
	require.Equal(t, "hello", value)
}

// executedPipeline builds a result map keyed at partition 0 holding the given
// command and returns a pipeline whose ctx is already canceled (i.e. Exec has
// completed) so that Result() reads the response instead of returning ErrNotReady.
func executedPipeline(cmd redis.Cmder) (*DMapPipeline, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dp := &DMapPipeline{
		result: map[uint64][]redis.Cmder{0: {cmd}},
	}
	return dp, ctx
}

func TestFutureDelete_Result_Error(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.del", "mydmap", "key")
	cmd.SetErr(redis.Nil)
	dp, ctx := executedPipeline(cmd)

	f := &FutureDelete{dp: dp, ctx: ctx, closedCtx: context.Background()}
	num, err := f.Result()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Equal(t, 0, num)
}

func TestFutureIncr_Result_Error(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.incr", "mydmap", "key")
	cmd.SetErr(redis.Nil)
	dp, ctx := executedPipeline(cmd)

	f := &FutureIncr{dp: dp, ctx: ctx, closedCtx: context.Background()}
	num, err := f.Result()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Equal(t, 0, num)
}

func TestFutureDecr_Result_Error(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.decr", "mydmap", "key")
	cmd.SetErr(redis.Nil)
	dp, ctx := executedPipeline(cmd)

	f := &FutureDecr{dp: dp, ctx: ctx, closedCtx: context.Background()}
	num, err := f.Result()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Equal(t, 0, num)
}

func TestFutureIncrByFloat_Result_Error(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.incrbyfloat", "mydmap", "key")
	cmd.SetErr(redis.Nil)
	dp, ctx := executedPipeline(cmd)

	f := &FutureIncrByFloat{dp: dp, ctx: ctx, closedCtx: context.Background()}
	val, err := f.Result()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Equal(t, float64(0), val)
}

func TestFutureIncrByFloat_Result_NilValue(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.incrbyfloat", "mydmap", "key")
	cmd.SetVal(nil)
	dp, ctx := executedPipeline(cmd)

	f := &FutureIncrByFloat{dp: dp, ctx: ctx, closedCtx: context.Background()}
	val, err := f.Result()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Equal(t, float64(0), val)
}

func TestFutureIncrByFloat_Result_OK(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.incrbyfloat", "mydmap", "key")
	cmd.SetVal("3.14")
	dp, ctx := executedPipeline(cmd)

	f := &FutureIncrByFloat{dp: dp, ctx: ctx, closedCtx: context.Background()}
	val, err := f.Result()
	require.NoError(t, err)
	require.InDelta(t, 3.14, val, 0.0001)
}

func TestFutureDelete_Result_OK(t *testing.T) {
	cmd := redis.NewCmd(context.Background(), "dm.del", "mydmap", "key")
	cmd.SetVal(int64(1))
	dp, ctx := executedPipeline(cmd)

	f := &FutureDelete{dp: dp, ctx: ctx, closedCtx: context.Background()}
	num, err := f.Result()
	require.NoError(t, err)
	require.Equal(t, 1, num)
}
