package miner

import (
	"bufio"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/clique"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
)

var (
	testGMPTSenderAddress = common.HexToAddress("0x0000000000000000000000000000000000001111")
	testGMPTSenderKey, _  = crypto.GenerateKey()
	testGenesisAlloc      = make(core.GenesisAlloc)
	testSenderNounce      = make(map[common.Address]uint64)
	testTxList            = make([]*types.Transaction, 0)
)

func readEthTxns(t *testing.T, n int) {
	if testBankFunds == nil {
		t.Fatal("testBankFunds is nil")
	}
	testGenesisAlloc[testBankAddress] = core.GenesisAccount{Balance: testBankFunds}
	// for each item
	// TODO: read sender address
	// TODO: read other attributes
	ethDir := "/ethereum/transactions-sort/"
	ethFiles, err := os.ReadDir(ethDir)
	if err != nil {
		t.Fatal(err)
	}
	count := uint64(0)
	for i, f := range ethFiles {
		if i == 8 {
			break
		}
		if !f.IsDir() {
			path := ethDir + f.Name()
			file, err := os.Open(path)
			fmt.Printf("path: %v\n", path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1024*1024)
			scanner.Scan()
			for scanner.Scan() {
				line := scanner.Text()
				nounce := testSenderNounce[parseGMPTTxSender(t, line)]
				testSenderNounce[parseGMPTTxSender(t, line)] = nounce + 1
				testTxList = append(testTxList, parseGMPTTx(t, line, nounce))
				testGenesisAlloc[parseGMPTTxSender(t, line)] = core.GenesisAccount{Balance: testBankFunds}
				// count
				count++
				if count == uint64(n) {
					return
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func parseGMPTTx(t *testing.T, line string, nounce uint64) *types.Transaction {
	// Read from csv line
	lineSplit := strings.Split(line, ",")
	// nounce, err := strconv.ParseUint(lineSplit[1], 10, 64)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	fromAddress := common.HexToAddress(lineSplit[3])
	toAddress := common.HexToAddress(lineSplit[4])
	value, _ := new(big.Int).SetString(lineSplit[5], 10)
	if value == nil {
		t.Fatal("value is nil")
	}
	// gasLimit := params.TxGas
	// gasPrice := big.NewInt(10 * params.InitialBaseFee)
	gasLimit, err := strconv.ParseUint(lineSplit[6], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	gasPrice, _ := new(big.Int).SetString(lineSplit[7], 10)
	if gasPrice == nil {
		t.Fatal("gasPrice is nil")
	}

	tx := types.NewTransaction(
		nounce,         // nonce
		toAddress,      // toAddress
		value,          // Value
		gasLimit,       // [FIX] gas limit
		gasPrice,       // [FIX] gasPrice
		fromAddress[:], // TODO: for uinqueness
	)

	// TODO: ensure the sender key here should not be used
	// rndSenderKey, err := crypto.GenerateKey()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// TODO: signiture value should not be the same for each sender
	// or the hash might be the same
	tx, err = types.SignTx(tx, types.HomesteadSigner{}, testBankKey)
	if err != nil {
		t.Fatal(err)
	}
	tx.From.Store(types.SigCache{Signer: types.HomesteadSigner{}, From: fromAddress})
	// addr2, err := types.Sender(types.HomesteadSigner{}, tx)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// fmt.Printf("sender address after From.Store: %v\n", addr2)
	return tx
}

func parseGMPTTxReceiver(t *testing.T, line string) common.Address {
	toAddress := common.HexToAddress(strings.Split(line, ",")[4])
	return toAddress
}

func parseGMPTTxSender(t *testing.T, line string) common.Address {
	fromAddress := common.HexToAddress(strings.Split(line, ",")[3])
	return fromAddress
}

// ----------------------------------------------------------------------------
// Functions for GMPT ---------------------------------------------------------
// ----------------------------------------------------------------------------
func newGMPTTestWorkerBackend(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, n int) *testWorkerBackend {
	// genesisAlloc := make(core.GenesisAlloc)
	// TODO: read from csv and allocate balance for all sender accounts
	// TODO: 所有sender账户全部读进来，然后分配余额
	// genesisAlloc[testBankAddress] = core.GenesisAccount{Balance: testBankFunds}
	// genesisAlloc[testGMPTSenderAddress] = core.GenesisAccount{Balance: testBankFunds}
	var gspec = core.Genesis{
		GasLimit: params.MaxGasLimit, // TODO: Have set to max gas linit
		Config:   chainConfig,
		Alloc:    testGenesisAlloc, // TODO: pre-allocate accounts here
	}

	switch e := engine.(type) {
	case *clique.Clique:
		gspec.ExtraData = make([]byte, 32+common.AddressLength+crypto.SignatureLength)
		copy(gspec.ExtraData[32:32+common.AddressLength], testBankAddress.Bytes())
		e.Authorize(testBankAddress, func(account accounts.Account, s string, data []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(data), testBankKey)
		})
	case *ethash.Ethash:
	default:
		t.Fatalf("unexpected consensus engine type: %T", engine)
	}
	genesis := gspec.MustCommit(db)

	chain, _ := core.NewBlockChain(db, &core.CacheConfig{TrieDirtyDisabled: true}, gspec.Config, engine, vm.Config{}, nil, nil)
	txpool := core.NewTxPool(testTxPoolConfig, chainConfig, chain)

	// Generate a small n-block chain and an uncle block for it
	if n > 0 {
		blocks, _ := core.GenerateChain(chainConfig, genesis, engine, db, n, func(i int, gen *core.BlockGen) {
			gen.SetCoinbase(testBankAddress)
		})
		if _, err := chain.InsertChain(blocks); err != nil {
			t.Fatalf("failed to insert origin chain: %v", err)
		}
	}
	parent := genesis
	if n > 0 {
		parent = chain.GetBlockByHash(chain.CurrentBlock().ParentHash())
	}
	blocks, _ := core.GenerateChain(chainConfig, parent, engine, db, 1, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(testUserAddress)
	})

	return &testWorkerBackend{
		db:         db,
		chain:      chain,
		txPool:     txpool,
		genesis:    &gspec,
		uncleBlock: blocks[0],
	}
}

func newGMPTTestWorker(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, blocks int) (*worker, *testWorkerBackend) {
	backend := newGMPTTestWorkerBackend(t, chainConfig, engine, db, blocks)
	backend.txPool.AddLocals(pendingTxs)
	w := newWorker(&Config{
		Recommit: time.Second,
		GasCeil:  params.MaxGasLimit, // TODO: Have removed the gas limit
	}, chainConfig, engine, backend, new(event.TypeMux), nil, false)
	w.setEtherbase(testBankAddress) // TODO: what is this
	return w, backend
}

func newGMPTRandomTx(t *testing.T, nounce uint64) *types.Transaction {
	// transfer from bank to random user
	// TODO We do not know the key, we only knows the from address
	// Bank to user
	gasPrice := big.NewInt(10 * params.InitialBaseFee)

	// rnd user address
	rndUserKey, _ := crypto.GenerateKey()
	rndUserAddress := crypto.PubkeyToAddress(rndUserKey.PublicKey)

	tx := types.NewTransaction(
		nounce,               // nonce
		rndUserAddress,       // TODO to
		big.NewInt(10000000), // TODO [MOD] value modified
		params.TxGas,         // [FIX] gas limit
		gasPrice,             // [FIX] gasPrice
		nil,                  // TODO [MOD] data
	)
	// fmt.Printf("tx: %v\n", tx)
	// sign with random user key
	tx, err := types.SignTx(tx, types.HomesteadSigner{}, testGMPTSenderKey)
	// TODO!!
	tx.From.Store(types.SigCache{Signer: types.HomesteadSigner{}, From: testGMPTSenderAddress})
	addr2, _ := types.Sender(types.HomesteadSigner{}, tx)
	fmt.Printf("sender address after From.Store: %v\n", addr2)

	// signiture (v, r, s) is set
	// hash, size, from is not set
	if err != nil {
		t.Fatal(err)
	}
	// fmt.Printf("tx: %v\n", tx)
	return tx
}

func TestGMPTNewGMPTTx(t *testing.T) {
	tx := newGMPTRandomTx(t, 0)
	fmt.Printf("tx: %v\n", tx)
}

func TestGMPTTransactionProcessing(t *testing.T) {
	var (
		engine      consensus.Engine
		chainConfig *params.ChainConfig
		db          = rawdb.NewMemoryDatabase() // TODO: disk?
	)

	// read data
	readEthTxns(t, 1000)
	// for _, tx := range testTxList {
	// 	fmt.Printf("tx: %v\n sender: %v\n", tx, tx.From.Load().(types.SigCache).From)
	// }
	// for addr, _ := range testGenesisAlloc {
	// 	fmt.Printf("addr: %v\n", addr)
	// }

	chainConfig = params.AllEthashProtocolChanges
	engine = ethash.NewFaker()

	chainConfig.LondonBlock = big.NewInt(0)
	w, b := newGMPTTestWorker(t, chainConfig, engine, db, 0)
	defer w.close()

	// This test chain imports the mined blocks.
	db2 := rawdb.NewMemoryDatabase()
	b.genesis.MustCommit(db2)
	chain, _ := core.NewBlockChain(db2, nil, b.chain.Config(), engine, vm.Config{}, nil, nil)
	defer chain.Stop()

	// Ignore empty commit here for less noise.
	w.skipSealHook = func(task *task) bool {
		return len(task.receipts) == 0
	}

	// Wait for mined blocks.
	sub := w.mux.Subscribe(core.NewMinedBlockEvent{})
	defer sub.Unsubscribe()

	// Start mining!
	w.start()

	errs := b.txPool.AddLocals(testTxList)
	for _, err := range errs {
		if err != nil {
			t.Fatalf("failed to add local transaction: %v", err)
		}
	}

	select {
	case ev := <-sub.Chan():
		block := ev.Data.(core.NewMinedBlockEvent).Block
		if _, err := chain.InsertChain([]*types.Block{block}); err != nil {
			t.Fatalf("failed to insert new mined block %d: %v", block.NumberU64(), err)
		}
		w.stateLock.Unlock()
	case <-time.After(10000 * time.Second): // Worker needs 1s to include new changes.
		t.Fatalf("timeout")
	}
}
