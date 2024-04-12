package trie

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/stretchr/testify/assert"
)

// !!! use TryGetHex and tryUpdateHex
const read_flag bool = true
const write_flag bool = false

func random_select_read_data(keys [][]byte, record_num int, lookup_num int) (rkeys [][]byte) {
	rand.Seed(time.Now().UnixNano())
	rkeys = make([][]byte, lookup_num)
	for i := 0; i < lookup_num; i++ {
		idx := rand.Int() % record_num
		rkeys[i] = keys[idx]
	}
	return rkeys
}

func data_gen() (keys_bytes, values_bytes []*bytes.Buffer) {
	n := 2
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

type Dataset int64

const (
	WIKI Dataset = iota
	YCSB
	ETH
	LOOKUP
	TRIESIZE
	RW
	THREAD_NUM
	ZIPF
)

func get_record_num(dataset Dataset, t *testing.T) int {
	var data_num_str string
	switch dataset {
	case WIKI:
		data_num_str = os.Getenv("GMPT_WIKI_DATA_VOLUME")
	case YCSB:
		data_num_str = os.Getenv("GMPT_YCSB_DATA_VOLUME")
	case ETH:
		data_num_str = os.Getenv("GMPT_ETH_DATA_VOLUME")
	case LOOKUP:
		data_num_str = os.Getenv("GMPT_DATA_LOOKUP_VOLUME")
	case TRIESIZE:
		data_num_str = os.Getenv("GMPT_TRIESIZE")
	case RW:
		data_num_str = os.Getenv("GMPT_RW_RRATIO")
	case THREAD_NUM:
		data_num_str = os.Getenv("GMPT_THREAD_NUM")
	case ZIPF:
		data_num_str = os.Getenv("GMPT_ZIPF")
	default:
		t.Fatalf("Wrong Dataset Type\n")
	}
	if len(data_num_str) == 0 {
		t.Fatalf("Failed to get the env variable")
	}
	if n, err := strconv.Atoi(data_num_str); err != nil {
		t.Fatalf("Number Error: %s", data_num_str)
		return -1
	} else {
		return n
	}
}
func TestDatagen(T *testing.T) {
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	keys, values := data_gen()

	for i := range keys {
		// TODO TryUpdateHex
		trie.Update(keys[i].Bytes(), values[i].Bytes())
	}
}

// names are declared as Benchmark+(name in C++ MPT project)
func TestPutBenchmark(t *testing.T) {
	// TODO: Use Hex
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

func TestTrieDBCommit(t *testing.T) {
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	keys, values := data_gen()
	for i := range keys {
		trie.Update(keys[i].Bytes(), values[i].Bytes())
		fmt.Printf("key %v, value %v...\n", keys[i].Bytes(), values[i].Bytes()[:2])
	}
	root, nodeset, err := trie.Commit(true)
	// node set 存储了:
	// 路径 ->inner node
	// [<leaf node, parent hash>]
	if err != nil {
		t.Fatal(err)
	}
	triedb.Update(NewWithNodeSet(nodeset))
	triedb.Commit(root, true, nil)
}

func TestHashBenchmark(t *testing.T) {
	// TODO: Use Hex
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

// func readWiki(t *testing.T) (keys, values [][]byte) {
// 	indexDir := "../../dataset/wiki/index/"
// 	valueDir := "../../dataset/wiki/value/"
// 	indexFiles, err := os.ReadDir(indexDir)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for _, f := range indexFiles {
// 		if !f.IsDir() {
// 			path := indexDir + f.Name()
// 			file, err := os.Open(path)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			defer file.Close()
// 			scanner := bufio.NewScanner(file)
// 			for scanner.Scan() {
// 				r, _ := regexp.Compile("^(.*:.*):.*$")
// 				k := r.FindStringSubmatch(scanner.Text())[1]
// 				keys = append(keys, keybytesToHex([]byte(k)))
// 			}
// 		}
// 	}
// 	fmt.Println(len(keys))
// 	valueFiles, err := os.ReadDir(valueDir)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for _, f := range valueFiles {
// 		if !f.IsDir() {
// 			path := valueDir + f.Name()
// 			doc := etree.NewDocument()
// 			if err := doc.ReadFromFile(path); err != nil {
// 				t.Fatal(err)
// 			}
// 			root := doc.Root()
// 			for _, page := range root.SelectElements("page") {
// 				pageDoc := etree.NewDocument()
// 				pageDoc.AddChild(page)
// 				value, err := pageDoc.WriteToString()
// 				if err != nil {
// 					t.Fatal(err)
// 				}
// 				values = append(values, []byte(value))
// 			}
// 		}
// 	}
// 	return keys, values
// }

func readWikiFast(t *testing.T) (keys, values [][]byte) {
	indexDir := "/wiki/index/"
	valueDir := "/wiki/value/"
	indexFiles, err := os.ReadDir(indexDir)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := regexp.Compile("^(.*:.*):.*$")
	for _, f := range indexFiles {
		if !f.IsDir() {
			// println(f.Name())
			path := indexDir + f.Name()
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				k := r.FindStringSubmatch(scanner.Text())[1]
				keys = append(keys, keybytesToHex([]byte(k)))
			}
		}
		// fmt.Printf("file %v, keys %d\n", f.Name(), len(keys))
	}
	// fmt.Println(len(keys))
	valueFiles, err := os.ReadDir(valueDir)
	if err != nil {
		t.Fatal(err)
	}

	rStart, _ := regexp.Compile("<page>")
	rEnd, _ := regexp.Compile("</page>")
	for _, f := range valueFiles {
		if !f.IsDir() {
			// println(f.Name())
			path := valueDir + f.Name()
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)

			const maxCapacity = 512 * 1024
			buf := make([]byte, maxCapacity)
			scanner.Buffer(buf, maxCapacity)

			value := make([]byte, 0)
			for scanner.Scan() {
				line := scanner.Text()
				// delete the tail spaces
				line = strings.TrimRight(line, " ")
				line += "\n"
				if rStart.MatchString(line) {
					value = []byte(line)
				} else if rEnd.MatchString(line) {
					value = append(value, []byte(line)...)
					value = bytes.Trim(value, " \n")
					value = append(value, 0x00)
					values = append(values, value)
				} else {
					value = append(value, []byte(line)...)
				}
			}
		}
		// fmt.Printf("file %v, values %d\n", f.Name(), len(values))
	}
	return keys, values
}

func TestPutWikiBench(t *testing.T) {
	// keys, values := readWiki(t)
	keys, values := readWikiFast(t)
	fmt.Println(len(keys), len(values))
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	n := 10000
	start := time.Now()
	for i := 0; i < n; i++ {
		// update using hex encoding
		trie.tryUpdateHex(keys[i], values[i])
	}
	end := time.Now()
	duration := end.Sub(start)
	fmt.Printf("%v elements\n", n)
	fmt.Printf("Ethereum puts execution time %d us, throughput %d qps\n", duration.Microseconds(), int64(n)*1000/duration.Microseconds()*1000)
}

func TestHashWikiBench(t *testing.T) {
	keys, values := readWikiFast(t)
	fmt.Println(len(keys), len(values))
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	// n := 10000
	n := len(keys)
	for i := 0; i < n; i++ {
		trie.tryUpdateHex(keys[i], values[i])
	}
	start := time.Now()
	trie.Hash()
	end := time.Now()
	duration := end.Sub(start)
	fmt.Printf("%v elements\n", n)
	fmt.Printf("Ethereum hash execution time %d us, throughput %d qps\n", duration.Microseconds(), int64(n)*1000/duration.Microseconds()*1000)
}

func readYcsb(file_name string, t *testing.T) (wkeys, wvalues, rkeys [][]byte) {
	path := "/ycsb/" + file_name
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
			wkeys = append(wkeys, keybytesToHex([]byte(key)))
			wvalues = append(wvalues, []byte(value))
		case "READ":
			key := remain
			rkeys = append(rkeys, keybytesToHex([]byte(key)))
		default:
			t.Fatalf("Wrong operation %v\n", op)
		}
	}
	return wkeys, wvalues, rkeys
}

func readYcsbRW(t *testing.T, tsize, ratio int) (rwkeys, rwvalues, bkeys, bvalues [][]byte, rwflags []bool) {
	path := "../../dataset/ycsb/ycsb_r" + strconv.Itoa(ratio)+ ".txt"
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		opEnd := strings.IndexByte(line, ' ')
		op := line[:opEnd]
		remain := line[opEnd+1:]
		if i < tsize {
			assert.Equal(t, op, "INSERT")
			kEnd := strings.IndexByte(remain, ' ')
			key := remain[:kEnd]
			value := remain[kEnd+1:]
			bkeys = append(bkeys, keybytesToHex([]byte(key)))
			bvalues = append(bvalues, []byte(value))
		} else {
			switch op {
			case "INSERT":
				kEnd := strings.IndexByte(remain, ' ')
				key := remain[:kEnd]
				value := remain[kEnd+1:]
				rwkeys = append(rwkeys, keybytesToHex([]byte(key)))
				rwvalues = append(rwvalues, []byte(value))
				rwflags = append(rwflags, write_flag)
			case "UPDATE":
				kEnd := strings.IndexByte(remain, ' ')
				key := remain[:kEnd]
				value := remain[kEnd+1:]
				rwkeys = append(rwkeys, keybytesToHex([]byte(key)))
				rwvalues = append(rwvalues, []byte(value))
				rwflags = append(rwflags, write_flag)
			case "READ":
				key := remain
				value := "EOF"
				rwkeys = append(rwkeys, keybytesToHex([]byte(key)))
				rwvalues = append(rwvalues, []byte(value))
				rwflags = append(rwflags, read_flag)
			default:
				t.Fatalf("Wrong operation %v\n", op)
			}
		}
		i++
	}
	return rwkeys, rwvalues, bkeys, bvalues, rwflags
}

func readYcsbRW1(t *testing.T, tsize int, path string) (rwkeys, rwvalues, bkeys, bvalues [][]byte, rwflags []bool) {
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		opEnd := strings.IndexByte(line, ' ')
		op := line[:opEnd]
		remain := line[opEnd+1:]
		if i < tsize {
			assert.Equal(t, op, "INSERT")
			kEnd := strings.IndexByte(remain, ' ')
			key := remain[:kEnd]
			value := remain[kEnd+1:]
			bkeys = append(bkeys, keybytesToHex([]byte(key)))
			bvalues = append(bvalues, []byte(value))
		} else {
			switch op {
			case "INSERT":
				kEnd := strings.IndexByte(remain, ' ')
				key := remain[:kEnd]
				value := remain[kEnd+1:]
				rwkeys = append(rwkeys, keybytesToHex([]byte(key)))
				rwvalues = append(rwvalues, []byte(value))
				rwflags = append(rwflags, write_flag)
			case "UPDATE":
				kEnd := strings.IndexByte(remain, ' ')
				key := remain[:kEnd]
				value := remain[kEnd+1:]
				rwkeys = append(rwkeys, keybytesToHex([]byte(key)))
				rwvalues = append(rwvalues, []byte(value))
				rwflags = append(rwflags, write_flag)
			case "READ":
				key := remain
				value := "EOF"
				rwkeys = append(rwkeys, keybytesToHex([]byte(key)))
				rwvalues = append(rwvalues, []byte(value))
				rwflags = append(rwflags, read_flag)
			default:
				t.Fatalf("Wrong operation %v\n", op)
			}
		}
		i++
	}
	return rwkeys, rwvalues, bkeys, bvalues, rwflags
}


func TestETEYCSBBench(t *testing.T) {
	wkeys, wvalues, rkeys := readYcsb("ycsb_insert_read.txt", t)
	fmt.Printf("Insert %d kv-pairs, Read %d k\n", len(wkeys), len(rkeys))

	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	nInsert := 50000
	nRead := len(rkeys)
	start := time.Now()
	for i := 0; i < nInsert; i++ {
		trie.tryUpdateHex(wkeys[i], wvalues[i])
	}
	trie.Hash()
	for _, rk := range rkeys {
		trie.TryGetHex(rk)
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
	for i, f := range ethFiles {
		if i == 8 {
			break
		}
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
				keys = append(keys, keybytesToHex(key))
				// keys = append(keys, key)
				values = append(values, value)
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return keys, values
}

func TestETEEthtxnBench(t *testing.T) {
	keys, values := readEthtxn(t)
	fmt.Println(len(keys), len(values))
	n := 64
	fmt.Printf("howmuch%d\n", n)
	fmt.Printf("max procs %v\n", runtime.GOMAXPROCS(0))

	{
		triedb := NewDatabase(rawdb.NewMemoryDatabase())
		trie := NewEmpty(triedb)
		// n := len(keys)
		valuesGet := make([][]byte, n)

		t1 := time.Now()
		for i := 0; i < n; i++ {
			// trie.Update(keys[i], values[i])
			trie.tryUpdateHex(keys[i], values[i])
			// trie.insert(trie.root, nil, keys[i], valueNode(values[i]))
		}
		t2 := time.Now()
		hash := trie.Hash()
		t3 := time.Now()
		trie.TryGetHexParallel(keys, valuesGet, n)
		t4 := time.Now()
		for i := range valuesGet {
			// fmt.Printf("%v\n%v\n\n", string(valuesGet[i]), string(values[i]))
			assert.Equal(t, string(valuesGet[i]), string(values[i]))
		}
		fmt.Printf("hash = %v\n", hash)

		duration := t4.Sub(t1)
		fmt.Printf("Ethereum Parallel execution time %d us, throughput %d qps [put: %d us] [hash: %d us] [get: %d us]\n", duration.Microseconds(), int64(n)*1000.0/duration.Microseconds()*1000.0, t2.Sub(t1).Microseconds(), t3.Sub(t2).Microseconds(), t4.Sub(t3).Microseconds())
	}
}

func TestHashRawEncode(t *testing.T) {
	triedb := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(triedb)
	keys := [3]string{"doe", "dog", "dogglesworth"}
	values := [3]string{"aaaaaaaaaaaa", "bbbbbbbbbbbb", "cccccccccccc"}
	for i := range keys {
		hex := keybytesToHex([]byte(keys[i]))
		fmt.Printf("%v\n", hex)
		value := []byte(values[i])
		trie.tryUpdateHex(hex, value)
	}

	// k := []byte("a")
	// v := []byte("b")
	// trie.tryUpdateHex(k, v)
	fmt.Printf("%v\n", trie.Hash())
}

func TestRLP(t *testing.T) {
	// value node
	vnode := valueNode([]byte("hello"))
	encVnode := nodeToBytes(vnode)
	fmt.Printf("value node: %v\n", encVnode)
	fmt.Printf("value node: %v\n", []byte("hello"))
	fnode := fullNode{}
	fnode.Children[1] = vnode
	encFnode := nodeToBytes(&fnode)
	fmt.Printf("full node: %v\n", encFnode)
}

func TestEthtxnRLP(t *testing.T) {
	keys, values := readEthtxn(t)
	fmt.Println(len(keys), len(values))
	n := 64
	fmt.Printf("howmuch%d\n", n)
	fmt.Printf("max procs %v\n", runtime.GOMAXPROCS(0))

	{
		triedb := NewDatabase(rawdb.NewMemoryDatabase())
		trie := NewEmpty(triedb)
		// n := len(keys)
		valuesGet := make([][]byte, n)

		t1 := time.Now()
		for i := 0; i < n; i++ {
			// trie.Update(keys[i], values[i])
			trie.tryUpdateHex(keys[i], values[i])
			// trie.insert(trie.root, nil, keys[i], valueNode(values[i]))
		}
		t2 := time.Now()
		hash := trie.Hash()
		fmt.Printf("root hash: %v\n", hash.Hex())
		t3 := time.Now()
		trie.TryGetHexParallel(keys, valuesGet, n)
		t4 := time.Now()
		for i := range valuesGet {
			// fmt.Printf("%v\n%v\n\n", string(valuesGet[i]), string(values[i]))
			assert.Equal(t, string(valuesGet[i]), string(values[i]))
		}

		duration := t4.Sub(t1)
		fmt.Printf("Ethereum Parallel execution time %d us, throughput %d qps [put: %d us] [hash: %d us] [get: %d us]\n", duration.Microseconds(), int64(n)*1000.0/duration.Microseconds()*1000.0, t2.Sub(t1).Microseconds(), t3.Sub(t2).Microseconds(), t4.Sub(t3).Microseconds())
	}

}
func TestCompactAndHex(t *testing.T) {
	// hex to compact and compact to hex
	key := []byte("abcdefghigklmnopqrstuvwxyz")
	keyhex := keybytesToHex(key)
	fmt.Println(keyhex)
	l := hexToCompactInPlace(keyhex)
	keyhex2 := compactToHex(keyhex[:l])
	fmt.Println(keyhex2)
}
