package trie

import (
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/stretchr/testify/assert"
)

// !!! use TryGetHex and tryUpdateHex
func TestInsertWiki(t *testing.T) {
	keys, values := readWikiFast(t)
	assert.Equal(t, len(keys), len(values))
	insert_num := get_record_num(WIKI, t)
	assert.LessOrEqual(t, insert_num, len(keys))

	fmt.Printf("Inserting %d k-v pairs\n", insert_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)

	t1 := time.Now()
	for i := 0; i < insert_num; i++ {
		trie.tryUpdateHex(keys[i], values[i])
	}
	t2 := time.Now()
	hash := trie.Hash()
	t3 := time.Now()

	us := t3.Sub(t1).Microseconds()
	insert_us := t2.Sub(t1).Microseconds()
	hash_us := t3.Sub(t2).Microseconds()

	fmt.Printf("Ethereum hash result: %v\n", hash)
	fmt.Printf("Ethereum e2e throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/us, insert_num, 0)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
}

func TestInsertYCSB(t *testing.T) {
	keys, values, _ := readYcsb(t)
	assert.Equal(t, len(keys), len(values))
	insert_num := get_record_num(YCSB, t)
	assert.LessOrEqual(t, insert_num, len(keys))

	fmt.Printf("Inserting %d k-v pairs\n", insert_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)

	t1 := time.Now()
	for i := 0; i < insert_num; i++ {
		trie.tryUpdateHex(keys[i], values[i])
	}
	t2 := time.Now()
	hash := trie.Hash()
	t3 := time.Now()

	us := t3.Sub(t1).Microseconds()
	insert_us := t2.Sub(t1).Microseconds()
	hash_us := t3.Sub(t2).Microseconds()

	fmt.Printf("Ethereum hash result: %v\n", hash)
	fmt.Printf("Ethereum e2e throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/us, insert_num, 0)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
}

func TestInsertEthtxn(t *testing.T) {
	keys, values := readEthtxn(t)
	assert.Equal(t, len(keys), len(values))
	insert_num := get_record_num(ETH, t)
	assert.LessOrEqual(t, insert_num, len(keys))

	fmt.Printf("Inserting %d k-v pairs\n", insert_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)

	t1 := time.Now()
	for i := 0; i < insert_num; i++ {
		trie.tryUpdateHex(keys[i], values[i])
	}
	t2 := time.Now()
	hash := trie.Hash()
	t3 := time.Now()

	us := t3.Sub(t1).Microseconds()
	insert_us := t2.Sub(t1).Microseconds()
	hash_us := t3.Sub(t2).Microseconds()

	fmt.Printf("Ethereum hash result: %v\n", hash)
	fmt.Printf("Ethereum e2e throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/us, insert_num, 0)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
}

func TestLookupWikiParallel(t *testing.T) {
	wkeys, wvalues := readWikiFast(t)
	record_num := get_record_num(WIKI, t)
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))

	rkeys := random_select_read_data(wkeys, record_num, lookup_num)
	assert.Equal(t, len(rkeys), lookup_num)

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs \n", record_num,
		lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	valuesGet := make([][]byte, lookup_num)

	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i], wvalues[i])
	}
	// TODO: hash or not?
	// trie.Hash()

	t1 := time.Now()
	trie.TryGetHexParallel(rkeys, valuesGet, lookup_num)
	t2 := time.Now()
	us := t2.Sub(t1).Microseconds()

	fmt.Printf("Ethereum parallel lookup response time: %d us for %d operations and trie with %d records\n", us, lookup_num, record_num)
}

func TestLookupYCSBParallel(t *testing.T) {
	wkeys, wvalues, rkeys := readYcsb(t)
	assert.Equal(t, len(wkeys), len(wvalues))

	record_num := get_record_num(YCSB, t)
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))
	assert.LessOrEqual(t, lookup_num, len(rkeys))

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs\n", record_num, lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	valuesGet := make([][]byte, lookup_num)

	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i], wvalues[i])
	}
	// TODO: hash or not?
	// trie.Hash()

	t1 := time.Now()
	trie.TryGetHexParallel(rkeys, valuesGet, lookup_num)
	t2 := time.Now()
	us := t2.Sub(t1).Microseconds()

	fmt.Printf("Ethereum parallel lookup response time: %d us for %d operations and trie with %d records\n", us, lookup_num, record_num)
}

func TestLookupEthtxnParallel(t *testing.T) {
	wkeys, wvalues := readEthtxn(t)
	record_num := get_record_num(ETH, t)
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))

	rkeys := random_select_read_data(wkeys, record_num, lookup_num)
	assert.Equal(t, len(rkeys), lookup_num)

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs \n", record_num,
		lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	valuesGet := make([][]byte, lookup_num)

	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i], wvalues[i])
	}
	// TODO: hash or not?
	// trie.Hash()

	t1 := time.Now()
	trie.TryGetHexParallel(rkeys, valuesGet, lookup_num)
	t2 := time.Now()
	us := t2.Sub(t1).Microseconds()

	fmt.Printf("Ethereum parallel lookup response time: %d us for %d operations and trie with %d records\n", us, lookup_num, record_num)
}

func TestLookupEthtxn(t *testing.T) {
	wkeys, wvalues := readEthtxn(t)
	record_num := get_record_num(ETH, t)
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))

	rkeys := random_select_read_data(wkeys, record_num, lookup_num)
	assert.Equal(t, len(rkeys), lookup_num)

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs \n", record_num,
		lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	// valuesGet := make([][]byte, lookup_num)

	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i], wvalues[i])
	}
	// TODO: hash or not?
	// trie.Hash()

	t1 := time.Now()
	for _, rk := range rkeys {
		trie.TryGetHex(rk)
	}
	t2 := time.Now()
	us := t2.Sub(t1).Microseconds()

	fmt.Printf("Ethereum lookup response time: %d us for %d operations and trie with %d records\n", us, lookup_num, record_num)
}

func TestLookupWiki(t *testing.T) {
	wkeys, wvalues := readWikiFast(t)
	record_num := get_record_num(WIKI, t)
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))

	rkeys := random_select_read_data(wkeys, record_num, lookup_num)
	assert.Equal(t, len(rkeys), lookup_num)

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs \n", record_num,
		lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	// valuesGet := make([][]byte, lookup_num)

	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i], wvalues[i])
	}
	// TODO: hash or not?
	// trie.Hash()

	t1 := time.Now()
	for _, rk := range rkeys {
		trie.TryGetHex(rk)
	}
	t2 := time.Now()
	us := t2.Sub(t1).Microseconds()

	fmt.Printf("Ethereum lookup response time: %d us for %d operations and trie with %d records\n", us, lookup_num, record_num)
}

func TestLookupYCSB(t *testing.T) {
	wkeys, wvalues, rkeys := readYcsb(t)
	assert.Equal(t, len(wkeys), len(wvalues))

	record_num := get_record_num(YCSB, t)
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))
	assert.LessOrEqual(t, lookup_num, len(rkeys))

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs\n", record_num, lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	// valuesGet := make([][]byte, lookup_num)

	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i], wvalues[i])
	}
	// TODO: hash or not?
	// trie.Hash()

	t1 := time.Now()
	for _, rk := range rkeys {
		trie.TryGetHex(rk)
	}
	t2 := time.Now()
	us := t2.Sub(t1).Microseconds()

	fmt.Printf("Ethereum lookup response time: %d us for %d operations and trie with %d records\n", us, lookup_num, record_num)
}
