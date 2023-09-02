package trie

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/stretchr/testify/assert"
)

func record_data(filename string, data []string) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	err = writer.Write(data)
	if err != nil {
		panic(err)
	}
	writer.Flush()
}

// !!! use TryGetHex and tryUpdateHex
func TestInsertWiki(t *testing.T) {
	thread_num := get_record_num(THREAD_NUM, t)
	if thread_num > runtime.NumCPU() {
		thread_num = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(thread_num)
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
	data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/us))}
	record_data("../../data/e2e_wiki_thread"+strconv.Itoa(thread_num)+".csv", data)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	insert_data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/insert_us))}
	record_data("../../data/insert_wiki_thread"+strconv.Itoa(thread_num)+".csv", insert_data)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
	hash_data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/hash_us))}
	record_data("../../data/hash_wiki_thread"+strconv.Itoa(thread_num)+".csv", hash_data)
}

func TestInsertYCSB(t *testing.T) {
	// fmt.Printf("CPU numbers: %d",runtime.NumCPU())
	thread_num := get_record_num(THREAD_NUM, t)
	if thread_num > runtime.NumCPU() {
		thread_num = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(thread_num)
	keys, values, _ := readYcsb("ycsb_insert_read.txt", t)
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
	data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/us))}
	record_data("../../data/e2e_ycsb_thread"+strconv.Itoa(thread_num)+".csv", data)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	insert_data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/insert_us))}
	record_data("../../data/insert_ycsb_thread"+strconv.Itoa(thread_num)+".csv", insert_data)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
	hash_data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/hash_us))}
	record_data("../../data/hash_ycsb_thread"+strconv.Itoa(thread_num)+".csv", hash_data)
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
	data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/us))}
	record_data("../../data/e2e_eth.csv", data)
	fmt.Printf("Ethereum Insert throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/insert_us, insert_num, 0)
	insert_data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/insert_us))}
	record_data("../../data/insert_eth.csv", insert_data)
	fmt.Printf("Ethereum hash throughput: %d qps for %d operations and trie with %d records\n", int64(insert_num)*1000000/hash_us, insert_num, 0)
	hash_data := []string{"Ethereum", strconv.Itoa(insert_num), strconv.Itoa(int(int64(insert_num)*1000000/hash_us))}
	record_data("../../data/hash_eth.csv", hash_data)
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
	wkeys, wvalues, all_rkeys := readYcsb("read.txt", t)
	assert.Equal(t, len(wkeys), len(wvalues))

	record_num := 640000
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

	record_data("../../data/lookup_ycsb.csv", []string{"CPU", strconv.Itoa(lookup_num), strconv.Itoa(int(us))})

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
	wkeys, wvalues, all_rkeys := readYcsb("ycsb_insert_read.txt", t)
	assert.Equal(t, len(wkeys), len(wvalues))

	record_num := 640000
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
	record_data("../../data/lookup_ycsb.csv", []string{"CPU", strconv.Itoa(lookup_num), strconv.Itoa(int(us))})

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
	record_num := 640000
	ratio := get_record_num(RW, t)
	rwkeys, rwvalues, bkeys, bvalues, rwflags := readYcsbRW(t, record_num, ratio)

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
	hash = trie.Hash()
	t2 := time.Now()
	us := t2.Sub(t1).Microseconds()
	fmt.Printf("Ethereum new hash result: %v\n", hash)
	fmt.Printf("valuesGet num: %d\n", read_num)
	fmt.Printf("Ethereum rw throughput: %d qps for %d operations and trie with %d records\n", int64(rw_num)*1000000/us, rw_num, 0)
	data := []string{"Ethereum", strconv.Itoa(ratio), strconv.Itoa(int(int64(rw_num)*1000000/us))}
	record_data("../../data/rw.csv", data)
}

func TestKeccak256(t *testing.T) {
	
}