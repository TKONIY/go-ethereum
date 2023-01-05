package trie

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/beevik/etree"
	"github.com/ethereum/go-ethereum/common"
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

func readWiki(t *testing.T) (keys, values [][]byte) {
	indexDir := "../../dataset/wiki/index/"
	valueDir := "../../dataset/wiki/value/"
	indexFiles, err := os.ReadDir(indexDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range indexFiles {
		if !f.IsDir() {
			path := indexDir + f.Name()
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				r, _ := regexp.Compile("^(.*:.*):.*$")
				k := r.FindStringSubmatch(scanner.Text())[1]
				keys = append(keys, []byte(k))
			}
		}
	}
	fmt.Println(len(keys))
	valueFiles, err := os.ReadDir(valueDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range valueFiles {
		if !f.IsDir() {
			path := valueDir + f.Name()
			doc := etree.NewDocument()
			if err := doc.ReadFromFile(path); err != nil {
				t.Fatal(err)
			}
			root := doc.Root()
			for _, page := range root.SelectElements("page") {
				pageDoc := etree.NewDocument()
				pageDoc.AddChild(page)
				value, err := pageDoc.WriteToString()
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, []byte(value))
			}
		}
	}
	return keys, values
}
func TestPutWikiBench(t *testing.T) {
	keys, values := readWiki(t)
	fmt.Println(len(keys), len(values))
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	n := 10000
	start := time.Now()
	for i := 0; i < n; i++ {
		trie.Update(keys[i], values[i])
	}
	end := time.Now()
	duration := end.Sub(start)
	fmt.Printf("%v elements\n", n)
	fmt.Printf("Ethereum puts execution time %d us, throughput %d qps\n", duration.Microseconds(), int64(n)*1000/duration.Microseconds()*1000)
}

func TestHashWikiBench(t *testing.T) {
	keys, values := readWiki(t)
	fmt.Println(len(keys), len(values))
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	// n := 10000
	n := len(keys)
	for i := 0; i < n; i++ {
		trie.Update(keys[i], values[i])
	}
	start := time.Now()
	trie.Hash()
	end := time.Now()
	duration := end.Sub(start)
	fmt.Printf("%v elements\n", n)
	fmt.Printf("Ethereum hash execution time %d us, throughput %d qps\n", duration.Microseconds(), int64(n)*1000/duration.Microseconds()*1000)
}

func readYcsb(t *testing.T, path string) (wkeys, wvalues, rkeys [][]byte) {
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		opEnd := strings.IndexByte(line, ' ')
		op := line[:opEnd]
		remain := line[opEnd+1:]
		switch op {
		case "INSERT":
			kEnd := strings.IndexByte(remain, ' ')
			key := remain[:kEnd]
			value := remain[kEnd+1:]
			wkeys = append(wkeys, []byte(key))
			wvalues = append(wvalues, []byte(value))
		case "READ":
			key := remain
			rkeys = append(rkeys, []byte(key))
		default:
			t.Fatalf("Wrong operation %v\n", op)
		}
	}
	return wkeys, wvalues, rkeys
}
func TestETEYCSBBench(t *testing.T) {
	wkeys, wvalues, rkeys := readYcsb(t, "../../dataset/ycsb/workloada.txt")
	fmt.Printf("Insert %d kv-pairs, Read %d k\n", len(wkeys), len(rkeys))

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	nInsert := 50000
	nRead := len(rkeys)
	start := time.Now()
	for i := 0; i < nInsert; i++ {
		trie.Update(wkeys[i], wvalues[i])
	}
	trie.Hash()
	for _, rk := range rkeys {
		trie.Get(rk)
	}
	end := time.Now()
	duration := end.Sub(start)
	fmt.Printf("Ethereum end-to-end execution time %d us, throughput %d qps\n", duration.Microseconds(), int64(nInsert+nRead)*1000/duration.Microseconds()*1000)
}

func readEthtxn(t *testing.T) (keys, values [][]byte) {
	ethDir := "/ethereum/transactions/"
	ethFiles, err := os.ReadDir(ethDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range ethFiles {
		if !f.IsDir() {
			path := ethDir + f.Name()
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1024*1024)
			scanner.Scan() // header
			for scanner.Scan() {
				line := scanner.Text()
				idx := strings.IndexByte(line, ',')
				hashHex := line[:idx][2:]
				key := common.Hex2Bytes(hashHex)
				value := []byte(line[idx+1:])
				keys = append(keys, key)
				values = append(values, value)
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
			break // only the first one
		}
	}
	return keys, values
}
func TestEthtxnBench(t *testing.T) {
	keys, values := readEthtxn(t)
	fmt.Println(len(keys), len(values))
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	// n := 10000
	n := len(keys)
	start := time.Now()
	for i := 0; i < n; i++ {
		trie.Update(keys[i], values[i])
	}
	end := time.Now()
	duration := end.Sub(start)
	fmt.Printf("%v elements\n", n)
	fmt.Printf("Ethereum put execution time %d us, throughput %d qps\n", duration.Microseconds(), int64(n)*1000/duration.Microseconds()*1000)
}
