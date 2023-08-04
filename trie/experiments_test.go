package trie

import (
	"fmt"
	"testing"
	"time"
	"math/rand"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/stretchr/testify/assert"
)

// !!! use TryGetHex and tryUpdateHex
func TestInsertWiki(t *testing.T) {
	keys, values := readWikiFast(t)
	assert.Equal(t, len(keys), len(values))
	insert_num := get_record_num(WIKI, t)
	// insert_num := 2500
	assert.LessOrEqual(t, insert_num, len(keys))

	fmt.Printf("Inserting %d k-v pairs\n", insert_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)

	random_start := rand.Intn(320000)
	t1 := time.Now()
	for i := 0; i < insert_num; i++ {
		trie.tryUpdateHex(keys[i+random_start], values[i+random_start])
	}
	t2 := time.Now()
	hash := trie.Hash()
	t3 := time.Now()

	us := t3.Sub(t1).Microseconds()
	insert_us := t2.Sub(t1).Microseconds()
	hash_us := t3.Sub(t2).Microseconds()
	fmt.Printf("response time: %d %d %d\n",us,insert_us,hash_us)
	fmt.Printf("Ethereum hash result: %v\n", hash)
	fmt.Printf("Ethereum e2e throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/us, insert_num, 0)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
}

func TestInsertYCSB(t *testing.T) {
	keys, values, _ := readYcsb(t)
	assert.Equal(t, len(keys), len(values))
	insert_num := get_record_num(YCSB, t)
	// insert_num:= 5000
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
	// insert_num := 5000
	assert.LessOrEqual(t, insert_num, len(keys))

	fmt.Printf("Inserting %d k-v pairs\n", insert_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)

	random_start := rand.Intn(640000)
	t1 := time.Now()
	for i := 0; i < insert_num; i++ {
		trie.tryUpdateHex(keys[i+random_start], values[i+random_start])
	}
	t2 := time.Now()
	hash := trie.Hash()
	t3 := time.Now()

	us := t3.Sub(t1).Microseconds()
	insert_us := t2.Sub(t1).Microseconds()
	hash_us := t3.Sub(t2).Microseconds()
	fmt.Printf("response time: %d %d %d\n",us,insert_us,hash_us)

	fmt.Printf("Ethereum hash result: %v\n", hash)
	fmt.Printf("Ethereum e2e throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/us, insert_num, 0)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
}

func TestLookupWikiParallel(t *testing.T) {
	wkeys, wvalues := readWikiFast(t)
	record_num := 320000
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))

	rkeys := random_select_read_data(wkeys, record_num, lookup_num)
	assert.Equal(t, len(rkeys), lookup_num)

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs \n", record_num,
		lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	valuesGet := make([][]byte, lookup_num)

	random_start := rand.Intn(320000)
	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i+random_start], wvalues[i+random_start])
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
	wkeys, wvalues, all_rkeys := readYcsb(t)
	assert.Equal(t, len(wkeys), len(wvalues))

	record_num := 1280000
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))
	assert.LessOrEqual(t, lookup_num, len(all_rkeys))
	rkeys := all_rkeys[0:lookup_num]
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
	record_num := 640000
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))

	rkeys := random_select_read_data(wkeys, record_num, lookup_num)
	assert.Equal(t, len(rkeys), lookup_num)

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs \n", record_num,
		lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	valuesGet := make([][]byte, lookup_num)

	random_start := rand.Intn(640000)
	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i+random_start], wvalues[i+random_start])
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
	record_num := 640000
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))

	rkeys := random_select_read_data(wkeys, record_num, lookup_num)
	assert.Equal(t, len(rkeys), lookup_num)

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs \n", record_num,
		lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	// valuesGet := make([][]byte, lookup_num)

	random_start := rand.Intn(640000)
	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i+random_start], wvalues[i+random_start])
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
	record_num := 320000
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))

	rkeys := random_select_read_data(wkeys, record_num, lookup_num)
	assert.Equal(t, len(rkeys), lookup_num)

	fmt.Printf("Inserting %d k-v pairs, then Reading %d k-v pairs \n", record_num,
		lookup_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	// valuesGet := make([][]byte, lookup_num)

	random_start := rand.Intn(320000)
	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(wkeys[i+random_start], wvalues[i+random_start])
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
	wkeys, wvalues, all_rkeys := readYcsb(t)
	assert.Equal(t, len(wkeys), len(wvalues))

	record_num := 1280000
	lookup_num := get_record_num(LOOKUP, t)
	assert.LessOrEqual(t, record_num, len(wkeys))
	assert.LessOrEqual(t, lookup_num, len(all_rkeys))
	rkeys := all_rkeys[0:lookup_num]
	// fmt.Printf("length:%d\n", len(rkeys))
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

func TestEthTrieSize(t *testing.T) {
	keys, values := readEthtxn(t)
	assert.Equal(t, len(keys), len(values))
	total_num := 640000
	record_num := get_record_num(TRIESIZE, t)
	// record_num := 320000
	insert_num := total_num - record_num
	assert.LessOrEqual(t, insert_num, len(keys))

	fmt.Printf("Inserting %d k-v pairs to Trie with %d k-v pairs, then have %d k-v pairs\n", insert_num, record_num, total_num)

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)

	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(keys[i], values[i])
	}
	hash := trie.Hash()	
	fmt.Printf("Ethereum old hash result: %v\n", hash)	

	t1 := time.Now()
	for i := record_num; i < total_num; i++ {
		trie.tryUpdateHex(keys[i], values[i])
	}
	t2 := time.Now()
	hash = trie.Hash()
	t3 := time.Now()

	us := t3.Sub(t1).Microseconds()
	insert_us := t2.Sub(t1).Microseconds()
	hash_us := t3.Sub(t2).Microseconds()

	fmt.Printf("Ethereum new hash result: %v\n", hash)
	fmt.Printf("Ethereum e2e throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/us, insert_num, 0)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
}

func TestRW(t *testing.T) {
	record_num := 100000
	rwkeys, rwvalues, bkeys, bvalues, rwflags := readYcsbRW(t, record_num)

	assert.Equal(t, len(rwkeys), len(rwvalues))
	assert.Equal(t, len(rwkeys), len(rwflags))
	assert.Equal(t, len(bkeys), len(bvalues))
	rw_num := len(rwkeys)
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	valuesGet := make([][]byte, rw_num)
	for i := 0; i < record_num; i++ {
		trie.tryUpdateHex(bkeys[i], bvalues[i])
	}
	hash := trie.Hash()
	fmt.Printf("Ethereum old hash result: %v\n", hash)
	read_num := 0
	t1 := time.Now()
	for i := 0; i < rw_num; i++ {
		if rwflags[i] == read_flag {
			valueGet, _ := trie.TryGetHex(rwkeys[i])
			valuesGet[read_num] = valueGet
			read_num += 1
		} else if rwflags[i] == write_flag {
			trie.tryUpdateHex(rwkeys[i], rwvalues[i])
		} else {
			panic("wrong flag")
		}
	}
	t2 := time.Now()
	us := t2.Sub(t1).Microseconds()
	hash = trie.Hash()
	fmt.Printf("Ethereum new hash result: %v\n", hash)
	fmt.Printf("valuesGet num: %d\n", read_num)
	fmt.Printf("Ethereum rw throughput: %d qps for %d operations and trie with %d records\n", int64(rw_num)*1000000/us, rw_num, 0)
}

func TestKeccak256(t *testing.T) {
	
}