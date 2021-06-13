package blockstore

import (
	"context"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	syncds "github.com/daotl/go-datastore/sync"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
)

var exampleBlock = blocks.NewBlock([]byte("foo"))

func testArcCached(ctx context.Context, bs Blockstore) (*arccache, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	opts := DefaultCacheOpts()
	opts.HasBloomFilterSize = 0
	opts.HasBloomFilterHashes = 0
	bbs, err := CachedBlockstore(ctx, bs, opts)
	if err == nil {
		return bbs.(*arccache), nil
	}
	return nil, err
}

func createStores(t testing.TB) (*arccache, Blockstore, *callbackDatastore) {
	mapds, err := ds.NewMapDatastore(key.KeyTypeString)
	assert.NoError(t, err)
	cd := &callbackDatastore{f: func() {}, ds: mapds}
	bs := NewBlockstore(syncds.MutexWrap(cd))
	arc, err := testArcCached(context.TODO(), bs)
	if err != nil {
		t.Fatal(err)
	}
	return arc, bs, cd
}

func trap(message string, cd *callbackDatastore, t *testing.T) {
	cd.SetFunc(func() {
		t.Fatal(message)
	})
}
func untrap(cd *callbackDatastore) {
	cd.SetFunc(func() {})
}

func TestRemoveCacheEntryOnDelete(t *testing.T) {
	arc, _, cd := createStores(t)

	arc.Put(exampleBlock)

	cd.Lock()
	writeHitTheDatastore := false
	cd.Unlock()

	cd.SetFunc(func() {
		writeHitTheDatastore = true
	})

	arc.DeleteBlock(exampleBlock.Cid())
	arc.Put(exampleBlock)
	if !writeHitTheDatastore {
		t.Fail()
	}
}

func TestElideDuplicateWrite(t *testing.T) {
	arc, _, cd := createStores(t)

	arc.Put(exampleBlock)
	trap("write hit datastore", cd, t)
	arc.Put(exampleBlock)
}

func TestHasRequestTriggersCache(t *testing.T) {
	arc, _, cd := createStores(t)

	arc.Has(exampleBlock.Cid())
	trap("has hit datastore", cd, t)
	if has, err := arc.Has(exampleBlock.Cid()); has || err != nil {
		t.Fatal("has was true but there is no such block")
	}

	untrap(cd)
	err := arc.Put(exampleBlock)
	if err != nil {
		t.Fatal(err)
	}

	trap("has hit datastore", cd, t)

	if has, err := arc.Has(exampleBlock.Cid()); !has || err != nil {
		t.Fatal("has returned invalid result")
	}
}

func TestGetFillsCache(t *testing.T) {
	arc, _, cd := createStores(t)

	if bl, err := arc.Get(exampleBlock.Cid()); bl != nil || err == nil {
		t.Fatal("block was found or there was no error")
	}

	trap("has hit datastore", cd, t)

	if has, err := arc.Has(exampleBlock.Cid()); has || err != nil {
		t.Fatal("has was true but there is no such block")
	}
	if _, err := arc.GetSize(exampleBlock.Cid()); err != ErrNotFound {
		t.Fatal("getsize was true but there is no such block")
	}

	untrap(cd)

	if err := arc.Put(exampleBlock); err != nil {
		t.Fatal(err)
	}

	trap("has hit datastore", cd, t)

	if has, err := arc.Has(exampleBlock.Cid()); !has || err != nil {
		t.Fatal("has returned invalid result")
	}
	if blockSize, err := arc.GetSize(exampleBlock.Cid()); blockSize == -1 || err != nil {
		t.Fatal("getsize returned invalid result", blockSize, err)
	}
}

func TestGetAndDeleteFalseShortCircuit(t *testing.T) {
	arc, _, cd := createStores(t)

	arc.Has(exampleBlock.Cid())
	arc.GetSize(exampleBlock.Cid())

	trap("get hit datastore", cd, t)

	if bl, err := arc.Get(exampleBlock.Cid()); bl != nil || err != ErrNotFound {
		t.Fatal("get returned invalid result")
	}

	if arc.DeleteBlock(exampleBlock.Cid()) != nil {
		t.Fatal("expected deletes to be idempotent")
	}
}

func TestArcCreationFailure(t *testing.T) {
	if arc, err := newARCCachedBS(context.TODO(), nil, -1); arc != nil || err == nil {
		t.Fatal("expected error and no cache")
	}
}

func TestInvalidKey(t *testing.T) {
	arc, _, _ := createStores(t)

	bl, err := arc.Get(cid.Cid{})

	if bl != nil {
		t.Fatal("blocks should be nil")
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHasAfterSucessfulGetIsCached(t *testing.T) {
	arc, bs, cd := createStores(t)

	bs.Put(exampleBlock)

	arc.Get(exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	arc.Has(exampleBlock.Cid())
}

func TestGetSizeAfterSucessfulGetIsCached(t *testing.T) {
	arc, bs, cd := createStores(t)

	bs.Put(exampleBlock)

	arc.Get(exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	arc.GetSize(exampleBlock.Cid())
}

func TestGetSizeAfterSucessfulHas(t *testing.T) {
	arc, bs, _ := createStores(t)

	bs.Put(exampleBlock)
	has, err := arc.Has(exampleBlock.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("expected to have block")
	}

	if size, err := arc.GetSize(exampleBlock.Cid()); err != nil {
		t.Fatal(err)
	} else if size != len(exampleBlock.RawData()) {
		t.Fatalf("expected size %d, got %d", len(exampleBlock.RawData()), size)
	}
}

func TestGetSizeMissingZeroSizeBlock(t *testing.T) {
	arc, bs, cd := createStores(t)
	emptyBlock := blocks.NewBlock([]byte{})
	missingBlock := blocks.NewBlock([]byte("missingBlock"))

	bs.Put(emptyBlock)

	arc.Get(emptyBlock.Cid())

	trap("has hit datastore", cd, t)
	if blockSize, err := arc.GetSize(emptyBlock.Cid()); blockSize != 0 || err != nil {
		t.Fatal("getsize returned invalid result")
	}
	untrap(cd)

	arc.Get(missingBlock.Cid())

	trap("has hit datastore", cd, t)
	if _, err := arc.GetSize(missingBlock.Cid()); err != ErrNotFound {
		t.Fatal("getsize returned invalid result")
	}
}

func TestDifferentKeyObjectsWork(t *testing.T) {
	arc, bs, cd := createStores(t)

	bs.Put(exampleBlock)

	arc.Get(exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	cidstr := exampleBlock.Cid().String()

	ncid, err := cid.Decode(cidstr)
	if err != nil {
		t.Fatal(err)
	}

	arc.Has(ncid)
}

func TestPutManyCaches(t *testing.T) {
	t.Run("happy path PutMany", func(t *testing.T) {
		arc, _, cd := createStores(t)
		arc.PutMany([]blocks.Block{exampleBlock})

		trap("has hit datastore", cd, t)
		arc.Has(exampleBlock.Cid())
		arc.GetSize(exampleBlock.Cid())
		untrap(cd)
		arc.DeleteBlock(exampleBlock.Cid())

		arc.Put(exampleBlock)
		trap("PunMany has hit datastore", cd, t)
		arc.PutMany([]blocks.Block{exampleBlock})
	})

	t.Run("PutMany with duplicates", func(t *testing.T) {
		arc, _, cd := createStores(t)
		arc.PutMany([]blocks.Block{exampleBlock, exampleBlock})

		trap("has hit datastore", cd, t)
		arc.Has(exampleBlock.Cid())
		arc.GetSize(exampleBlock.Cid())
		untrap(cd)
		arc.DeleteBlock(exampleBlock.Cid())

		arc.Put(exampleBlock)
		trap("PunMany has hit datastore", cd, t)
		arc.PutMany([]blocks.Block{exampleBlock})
	})
}

func BenchmarkARCCacheConcurrentOps(b *testing.B) {
	// ~4k blocks seems high enough to be realistic,
	// but low enough to cause collisions.
	// Keep it as a power of 2, to simplify code below.
	const numBlocks = 4 << 10

	dummyBlocks := make([]blocks.Block, numBlocks)

	{
		// scope dummyRand to prevent its unsafe concurrent use below
		dummyRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := range dummyBlocks {
			dummy := make([]byte, 32)
			if _, err := io.ReadFull(dummyRand, dummy); err != nil {
				b.Fatal(err)
			}
			dummyBlocks[i] = blocks.NewBlock(dummy)
		}
	}

	// Each test begins with half the blocks present in the cache.
	// This allows test cases to have both hits and misses,
	// regardless of whether or not they do Puts.
	putHalfBlocks := func(arc *arccache) {
		for i, block := range dummyBlocks {
			if i%2 == 0 {
				if err := arc.Put(block); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	// We always mix just two operations at a time.
	const numOps = 2
	var testOps = []struct {
		name string
		ops  [numOps]func(*arccache, blocks.Block)
	}{
		{"PutDelete", [...]func(*arccache, blocks.Block){
			func(arc *arccache, block blocks.Block) {
				arc.Put(block)
			},
			func(arc *arccache, block blocks.Block) {
				arc.DeleteBlock(block.Cid())
			},
		}},
		{"GetDelete", [...]func(*arccache, blocks.Block){
			func(arc *arccache, block blocks.Block) {
				arc.Get(block.Cid())
			},
			func(arc *arccache, block blocks.Block) {
				arc.DeleteBlock(block.Cid())
			},
		}},
		{"GetPut", [...]func(*arccache, blocks.Block){
			func(arc *arccache, block blocks.Block) {
				arc.Get(block.Cid())
			},
			func(arc *arccache, block blocks.Block) {
				arc.Put(block)
			},
		}},
	}

	for _, test := range testOps {
		test := test // prevent reuse of the range var
		b.Run(test.name, func(b *testing.B) {
			arc, _, _ := createStores(b)
			putHalfBlocks(arc)
			var opCounts [numOps]uint64

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					n := rnd.Int63()
					blockIdx := n % numBlocks         // lower bits decide the block
					opIdx := (n / numBlocks) % numOps // higher bits decide what operation

					block := dummyBlocks[blockIdx]
					op := test.ops[opIdx]
					op(arc, block)

					atomic.AddUint64(&opCounts[opIdx], 1)
				}
			})

			// We expect each op to fire roughly an equal amount of times.
			// Error otherwise, as that likely means the logic is wrong.
			var minIdx, maxIdx int
			var minCount, maxCount uint64
			for opIdx, count := range opCounts {
				if minCount == 0 || count < minCount {
					minIdx = opIdx
					minCount = count
				}
				if maxCount == 0 || count > maxCount {
					maxIdx = opIdx
					maxCount = count
				}
			}
			// Skip this check if we ran few times, to avoid false positives.
			if maxCount > 100 {
				ratio := float64(maxCount) / float64(minCount)
				if maxRatio := 2.0; ratio > maxRatio {
					b.Fatalf("op %d ran %fx as many times as %d", maxIdx, ratio, minIdx)
				}
			}

		})
	}
}
