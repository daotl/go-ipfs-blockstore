package blockstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	ds "github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	dsq "github.com/daotl/go-datastore/query"
	syncds "github.com/daotl/go-datastore/sync"
	blocks "github.com/ipfs/go-block-format"
	"github.com/stretchr/testify/require"
)

func testBloomCached(ctx context.Context, bs Blockstore) (*bloomcache, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts := DefaultCacheOpts()
	opts.HasARCCacheSize = 0
	bbs, err := CachedBlockstore(ctx, bs, opts)
	if err == nil {
		return bbs.(*bloomcache), nil
	}
	return nil, err
}

func TestPutManyAddsToBloom(t *testing.T) {
	mapds, err := ds.NewMapDatastore(key.KeyTypeString)
	require.NoError(t, err)
	bs := NewBlockstore(syncds.MutexWrap(mapds))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("Failed while waiting for the filter to build: %d", cachedbs.bloom.ElementsAdded())
	}

	block1 := blocks.NewBlock([]byte("foo"))
	block2 := blocks.NewBlock([]byte("bar"))
	emptyBlock := blocks.NewBlock([]byte{})

	cachedbs.PutMany(bg, []blocks.Block{block1, emptyBlock})
	has, err := cachedbs.Has(bg, block1.Cid())
	if err != nil {
		t.Fatal(err)
	}
	blockSize, err := cachedbs.GetSize(bg, block1.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if blockSize == -1 || !has {
		t.Fatal("added block is reported missing")
	}

	has, err = cachedbs.Has(bg, block2.Cid())
	if err != nil {
		t.Fatal(err)
	}
	blockSize, err = cachedbs.GetSize(bg, block2.Cid())
	if err != nil && err != ErrNotFound {
		t.Fatal(err)
	}
	if blockSize > -1 || has {
		t.Fatal("not added block is reported to be in blockstore")
	}

	has, err = cachedbs.Has(bg, emptyBlock.Cid())
	if err != nil {
		t.Fatal(err)
	}
	blockSize, err = cachedbs.GetSize(bg, emptyBlock.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if blockSize != 0 || !has {
		t.Fatal("added block is reported missing")
	}
}

func TestReturnsErrorWhenSizeNegative(t *testing.T) {
	mapds, err := ds.NewMapDatastore(key.KeyTypeString)
	require.NoError(t, err)
	bs := NewBlockstore(syncds.MutexWrap(mapds))
	_, err = bloomCached(context.Background(), bs, -1, 1)
	if err == nil {
		t.Fail()
	}
}
func TestHasIsBloomCached(t *testing.T) {
	mapds, err := ds.NewMapDatastore(key.KeyTypeString)
	require.NoError(t, err)
	cd := &callbackDatastore{f: func() {}, ds: mapds}
	bs := NewBlockstore(syncds.MutexWrap(cd))

	for i := 0; i < 1000; i++ {
		bs.Put(bg, blocks.NewBlock([]byte(fmt.Sprintf("data: %d", i))))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("Failed while waiting for the filter to build: %d", cachedbs.bloom.ElementsAdded())
	}

	cacheFails := 0
	cd.SetFunc(func() {
		cacheFails++
	})

	for i := 0; i < 1000; i++ {
		cachedbs.Has(bg, blocks.NewBlock([]byte(fmt.Sprintf("data: %d", i+2000))).Cid())
	}

	if float64(cacheFails)/float64(1000) > float64(0.05) {
		t.Fatalf("Bloom filter has cache miss rate of more than 5%%")
	}

	cacheFails = 0
	block := blocks.NewBlock([]byte("newBlock"))

	cachedbs.PutMany(bg, []blocks.Block{block})
	if cacheFails != 2 {
		t.Fatalf("expected two datastore hits: %d", cacheFails)
	}
	cachedbs.Put(bg, block)
	if cacheFails != 3 {
		t.Fatalf("expected datastore hit: %d", cacheFails)
	}

	if has, err := cachedbs.Has(bg, block.Cid()); !has || err != nil {
		t.Fatal("has gave wrong response")
	}

	bl, err := cachedbs.Get(bg, block.Cid())
	if bl.String() != block.String() {
		t.Fatal("block data doesn't match")
	}

	if err != nil {
		t.Fatal("there should't be an error")
	}
}

var _ ds.Batching = (*callbackDatastore)(nil)

type callbackDatastore struct {
	sync.Mutex
	f  func()
	ds ds.Datastore
}

func (c *callbackDatastore) SetFunc(f func()) {
	c.Lock()
	defer c.Unlock()
	c.f = f
}

func (c *callbackDatastore) CallF() {
	c.Lock()
	defer c.Unlock()
	c.f()
}

func (c *callbackDatastore) Put(ctx context.Context, k key.Key, value []byte) (err error) {
	c.CallF()
	return c.ds.Put(ctx, k, value)
}

func (c *callbackDatastore) Get(ctx context.Context, k key.Key) (value []byte, err error) {
	c.CallF()
	return c.ds.Get(ctx, k)
}

func (c *callbackDatastore) Has(ctx context.Context, k key.Key) (exists bool, err error) {
	c.CallF()
	return c.ds.Has(ctx, k)
}

func (c *callbackDatastore) GetSize(ctx context.Context, k key.Key) (size int, err error) {
	c.CallF()
	return c.ds.GetSize(ctx, k)
}

func (c *callbackDatastore) Close() error {
	return nil
}

func (c *callbackDatastore) Delete(ctx context.Context, k key.Key) (err error) {
	c.CallF()
	return c.ds.Delete(ctx, k)
}

func (c *callbackDatastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	c.CallF()
	return c.ds.Query(ctx, q)
}

func (c *callbackDatastore) Sync(ctx context.Context, k key.Key) error {
	c.CallF()
	return c.ds.Sync(ctx, k)
}

func (c *callbackDatastore) Batch(_ context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(c), nil
}
