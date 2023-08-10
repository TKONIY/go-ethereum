// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

// #include "libgmpt.h"
// #cgo LDFLAGS: -L. -lgmpt -L/usr/local/cuda/lib64 -lcudart -lstdc++ -lcryptopp -lm -L/usr/local/lib -ltbb -ltbbmalloc
import "C"

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
)

// hasherPool holds LegacyKeccak256 hashers for rlpHash.
var hasherPool = sync.Pool{
	New: func() interface{} { return sha3.NewLegacyKeccak256() },
}

// encodeBufferPool holds temporary encoder buffers for DeriveSha and TX encoding.
var encodeBufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

// rlpHash encodes x and hashes the encoded bytes.
func rlpHash(x interface{}) (h common.Hash) {
	sha := hasherPool.Get().(crypto.KeccakState)
	defer hasherPool.Put(sha)
	sha.Reset()
	rlp.Encode(sha, x)
	sha.Read(h[:])
	return h
}

// prefixedRlpHash writes the prefix into the hasher before rlp-encoding x.
// It's used for typed transactions.
func prefixedRlpHash(prefix byte, x interface{}) (h common.Hash) {
	sha := hasherPool.Get().(crypto.KeccakState)
	defer hasherPool.Put(sha)
	sha.Reset()
	sha.Write([]byte{prefix})
	rlp.Encode(sha, x)
	sha.Read(h[:])
	return h
}

// TrieHasher is the tool used to calculate the hash of derivable list.
// This is internal, do not use.
type TrieHasher interface {
	Reset()
	Update([]byte, []byte)
	Hash() common.Hash
}

// DerivableList is the input to DeriveSha.
// It is implemented by the 'Transactions' and 'Receipts' types.
// This is internal, do not use these methods.
type DerivableList interface {
	Len() int
	EncodeIndex(int, *bytes.Buffer)
}

func encodeForDerive(list DerivableList, i int, buf *bytes.Buffer) []byte {
	buf.Reset()
	list.EncodeIndex(i, buf)
	// It's really unfortunate that we need to do perform this copy.
	// StackTrie holds onto the values until Hash is called, so the values
	// written to it must not alias.
	return common.CopyBytes(buf.Bytes())
}

// DeriveShaGMPT creates the tree hashes of transactions and receipts in a block header using GPU

func keybytesToHex(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}

func DeriveShaGMPTMultiCore(list DerivableList, tname string) common.Hash {
	collectStart := time.Now()
	// TODO GMPT
	// keysHexs := make([]byte, 0, list.Len()*32)
	// keysHexsIndexs := make([]int32, 0, list.Len()*2)
	// values := make([]byte, 0, list.Len()*1000)
	// valuesIndexs := make([]int64, 0, list.Len()*2)
	// insert_num := int(0)

	// multi core
	// runtime.GOMAXPROCS(0)
	// GMPT do not require the order
	nThread := runtime.GOMAXPROCS(0) * 2
	wg := sync.WaitGroup{}

	allKeysHexs := make([][]byte, nThread)
	allKeysHexsIndexs := make([][]int32, nThread)
	allValues := make([][]byte, nThread)
	allValuesIndexs := make([][]int64, nThread)

	kernel := func(tid int, valueBuf *bytes.Buffer) {

		st := tid * (list.Len() / nThread)
		end := (tid + 1) * (list.Len() / nThread)
		if tid == nThread-1 {
			end = list.Len()
		}

		allKeysHexs[tid] = make([]byte, 0, (end-st)*32)
		allKeysHexsIndexs[tid] = make([]int32, 0, (end-st)*2)
		allValues[tid] = make([]byte, 0, (end-st)*1000)
		allValuesIndexs[tid] = make([]int64, 0, (end-st)*2)

		// keysHexs := make([]byte, 0, (end-st)*32)
		// keysHexsIndexs := make([]int32, 0, (end-st)*2)
		// values := make([]byte, 0, (end-st)*1000)
		// valuesIndexs := make([]int64, 0, (end-st)*2)

		indexBuf := []byte{}
		for i := st; i < end; i++ {
			indexBuf = rlp.AppendUint64(indexBuf[:0], uint64(i))
			keyHex := keybytesToHex(indexBuf)
			value := encodeForDerive(list, i, valueBuf)
			// keysHexsIndexs = append(keysHexsIndexs, int32(len(keysHexs)))
			// keysHexs = append(keysHexs, keyHex...)
			// keysHexsIndexs = append(keysHexsIndexs, int32(len(keysHexs)-1))
			// valuesIndexs = append(valuesIndexs, int64(len(values)))
			// values = append(values, value...)
			// valuesIndexs = append(valuesIndexs, int64(len(values)-1))
			allKeysHexsIndexs[tid] = append(allKeysHexsIndexs[tid], int32(len(allKeysHexs[tid])))
			allKeysHexs[tid] = append(allKeysHexs[tid], keyHex...)
			allKeysHexsIndexs[tid] = append(allKeysHexsIndexs[tid], int32(len(allKeysHexs[tid])-1))
			allValuesIndexs[tid] = append(allValuesIndexs[tid], int64(len(allValues[tid])))
			allValues[tid] = append(allValues[tid], value...)
			allValuesIndexs[tid] = append(allValuesIndexs[tid], int64(len(allValues[tid])-1))

		}
		// allKeysHexs[tid] = keysHexs
		// allKeysHexsIndexs[tid] = keysHexsIndexs
		// allValues[tid] = values
		// allValuesIndexs[tid] = valuesIndexs
		wg.Done()
	}

	for i := 0; i < nThread; i++ {
		valueBuf := encodeBufferPool.Get().(*bytes.Buffer)
		defer encodeBufferPool.Put(valueBuf)
		wg.Add(1)
		go kernel(i, valueBuf)
	}

	keysHexs := make([]byte, 0, list.Len()*32)
	keysHexsIndexs := make([]int32, 0, list.Len()*2)
	values := make([]byte, 0, list.Len()*1000)
	valuesIndexs := make([]int64, 0, list.Len()*2)

	wg.Wait()

	for i := 0; i < nThread; i++ {
		nextKeysHexs := allKeysHexs[i]
		nextKeysHexsIndexs := allKeysHexsIndexs[i]
		nextValues := allValues[i]
		nextValuesIndexs := allValuesIndexs[i]
		offsetKeysHexs := len(keysHexs)
		offsetValues := len(values)

		if offsetKeysHexs != 0 || offsetValues != 0 {
			for j := 0; j < len(nextKeysHexsIndexs); j++ {
				nextKeysHexsIndexs[j] += int32(offsetKeysHexs)
			}
			for j := 0; j < len(nextValuesIndexs); j++ {
				nextValuesIndexs[j] += int64(offsetValues)
			}
		}

		keysHexs = append(keysHexs, nextKeysHexs...)
		keysHexsIndexs = append(keysHexsIndexs, nextKeysHexsIndexs...)
		values = append(values, nextValues...)
		valuesIndexs = append(valuesIndexs, nextValuesIndexs...)
	}
	insert_num := len(keysHexsIndexs) / 2

	collectEnd := time.Now()
	fmt.Printf("[Timer] DeriveShaGMPT collect: %v\n", collectEnd.Sub(collectStart))

	fmt.Printf("pre-allocate: %v, final %v\n", list.Len()*1000, len(values))
	cStart := time.Now()
	var trieType uint32
	if tname == "txs" {
		trieType = C.TRANSACTION_TRIE
	} else {
		if tname != "receipts" {
			panic("tname must be txs or receipts")
		}
		trieType = C.RECEIPT_TRIE
	}
	hash := C.build_mpt_olc(
		trieType,
		(*C.uchar)(unsafe.Pointer(&keysHexs[0])),
		(*C.int)(unsafe.Pointer(&keysHexsIndexs[0])),
		(*C.uchar)(unsafe.Pointer(&values[0])),
		(*C.int64_t)(unsafe.Pointer(&valuesIndexs[0])),
		(**C.uchar)(C.NULL),
		C.int(insert_num))
	cEnd := time.Now()
	fmt.Println("[Timer] GMPTIntermediateRoot() cgo:", cEnd.Sub(cStart))
	mySlice := C.GoBytes(unsafe.Pointer(hash), common.HashLength)
	// print("Hash: ")
	// for _, b := range mySlice {
	// 	fmt.Printf("%02x", b)
	// }
	// println()
	ret := common.Hash{}
	copy(ret[:], mySlice)
	fmt.Println("GMPTIntermediateRoot() hash:", ret.Hex())
	return ret
}

func DeriveShaGMPT(list DerivableList, tname string) common.Hash {
	collectStart := time.Now()
	// TODO GMPT
	keysHexs := make([]byte, 0, list.Len()*32)
	keysHexsIndexs := make([]int32, 0, list.Len()*2)
	values := make([]byte, 0, list.Len()*1000)
	valuesIndexs := make([]int64, 0, list.Len()*2)
	insert_num := int(0)

	// start encoding
	valueBuf := encodeBufferPool.Get().(*bytes.Buffer)
	defer encodeBufferPool.Put(valueBuf)
	var indexBuf []byte
	for i := 0; i < list.Len(); i++ {
		indexBuf = rlp.AppendUint64(indexBuf[:0], uint64(i))
		keyHex := keybytesToHex(indexBuf)
		value := encodeForDerive(list, i, valueBuf)
		// append for GPU
		keysHexsIndexs = append(keysHexsIndexs, int32(len(keysHexs)))
		keysHexs = append(keysHexs, keyHex...)
		keysHexsIndexs = append(keysHexsIndexs, int32(len(keysHexs)-1))
		valuesIndexs = append(valuesIndexs, int64(len(values)))
		values = append(values, value...)
		valuesIndexs = append(valuesIndexs, int64(len(values)-1))
		insert_num++
	}
	collectEnd := time.Now()
	fmt.Printf("[Timer] DeriveShaGMPT: %v\n", collectEnd.Sub(collectStart))

	fmt.Printf("pre-allocate: %v, final %v\n", list.Len()*1000, len(values))
	cStart := time.Now()
	var trieType uint32
	if tname == "txs" {
		trieType = C.TRANSACTION_TRIE
	} else {
		if tname != "receipts" {
			panic("tname must be txs or receipts")
		}
		trieType = C.RECEIPT_TRIE
	}
	hash := C.build_mpt_olc(
		trieType,
		(*C.uchar)(unsafe.Pointer(&keysHexs[0])),
		(*C.int)(unsafe.Pointer(&keysHexsIndexs[0])),
		(*C.uchar)(unsafe.Pointer(&values[0])),
		(*C.int64_t)(unsafe.Pointer(&valuesIndexs[0])),
		(**C.uchar)(C.NULL),
		C.int(insert_num))
	cEnd := time.Now()
	fmt.Println("[Timer] GMPTIntermediateRoot() cgo:", cEnd.Sub(cStart))
	mySlice := C.GoBytes(unsafe.Pointer(hash), common.HashLength)
	print("Hash: ")
	for _, b := range mySlice {
		fmt.Printf("%02x", b)
	}
	println()
	ret := common.Hash{}
	copy(ret[:], mySlice)
	return ret
}

// DeriveSha creates the tree hashes of transactions and receipts in a block header.
func DeriveSha(list DerivableList, hasher TrieHasher) common.Hash {
	hasher.Reset()

	valueBuf := encodeBufferPool.Get().(*bytes.Buffer)
	defer encodeBufferPool.Put(valueBuf)

	// StackTrie requires values to be inserted in increasing hash order, which is not the
	// order that `list` provides hashes in. This insertion sequence ensures that the
	// order is correct.
	var indexBuf []byte
	for i := 1; i < list.Len() && i <= 0x7f; i++ {
		indexBuf = rlp.AppendUint64(indexBuf[:0], uint64(i))
		value := encodeForDerive(list, i, valueBuf)
		hasher.Update(indexBuf, value)
	}
	if list.Len() > 0 {
		indexBuf = rlp.AppendUint64(indexBuf[:0], 0)
		value := encodeForDerive(list, 0, valueBuf)
		hasher.Update(indexBuf, value)
	}
	for i := 0x80; i < list.Len(); i++ {
		indexBuf = rlp.AppendUint64(indexBuf[:0], uint64(i))
		value := encodeForDerive(list, i, valueBuf)
		hasher.Update(indexBuf, value)
	}
	return hasher.Hash()
}
