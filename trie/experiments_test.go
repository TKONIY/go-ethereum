package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func data_gen() (keys_bytes, values_bytes []*bytes.Buffer) {
	n := 1 << 16
	value_size := 10000

	keys_bytes = make([]*bytes.Buffer, n)
	values_bytes = make([]*bytes.Buffer, n)

	keys := make([]uint16, n)
	for i := range keys {
		keys[i] = uint16(i)
	}

	rand.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for i := range keys_bytes {
		keys_bytes[i] = new(bytes.Buffer)
		binary.Write(keys_bytes[i], binary.LittleEndian, keys[i])
	}
	for i := range values_bytes {
		values_bytes[i] = bytes.NewBuffer(make([]byte, value_size))
		rand.Read(values_bytes[i].Bytes())
	}
	return keys_bytes, values_bytes
}

func TestDatagen(T *testing.T) {
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	keys, values := data_gen()

	for i := range keys {
		trie.Update(keys[i].Bytes(), values[i].Bytes())
	}
}

// names are declared as Benchmark+(name in C++ MPT project)
func TestPutBenchmark(t *testing.T) {
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	keys, values := data_gen()
	start := time.Now()
	for i := range keys {
		trie.Update(keys[i].Bytes(), values[i].Bytes())
	}
	end := time.Now()
	duration := end.Sub(start)
	n := int64(len(keys))

	fmt.Printf("Ethereum put execution time %d us, throughput %d qps\n", duration.Microseconds(), n*1000/duration.Microseconds()*1000)
}

func TestHashBenchmark(t *testing.T) {
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	keys, values := data_gen()
	for i := range keys {
		trie.Update(keys[i].Bytes(), values[i].Bytes())
	}
	start := time.Now()
	trie.Hash()
	end := time.Now()
	duration := end.Sub(start)

	n := int64(len(keys))
	fmt.Printf("Ethereum hash execution time %d us, throughput %d qps\n", duration.Microseconds(), n*1000/duration.Microseconds()*1000)
}
