package miner

import (
	"fmt"
	"math/big"
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
)

// ----------------------------------------------------------------------------
// Functions for GMPT ---------------------------------------------------------
// ----------------------------------------------------------------------------
func newGMPTTestWorkerBackend(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, n int) *testWorkerBackend {
	genesisAlloc := make(core.GenesisAlloc)
	// TODO: read from csv and allocate balance for all sender accounts
	// TODO: 全部读进去还是只有涉及的读进去?
	genesisAlloc[testBankAddress] = core.GenesisAccount{Balance: testBankFunds}
	genesisAlloc[testGMPTSenderAddress] = core.GenesisAccount{Balance: testBankFunds}

	var gspec = core.Genesis{
		GasLimit: params.MaxGasLimit, // TODO: Have set to max gas linit
		Config:   chainConfig,
		Alloc:    genesisAlloc, // TODO: pre-allocate accounts here
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

func readEthtxns(t *testing.T, n int) ([]*types.Transaction, []*common.Address) {
	// TODO: read from csv
	return nil, nil
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

	// handle transactions
	txsList := make([]*types.Transaction, 0)
	for i := uint64(0); i < 3; i++ {
		// tx := b.newRandomTx(true)
		tx := newGMPTRandomTx(t, i) // TODO
		txsList = append(txsList, tx)
		// fmt.Println("tx", tx.Hash().Hex())
	}
	errs := b.txPool.AddLocals(txsList)
	for _, err := range errs {
		if err != nil {
			t.Fatalf("failed to add local transaction: %v", err)
		}
	}

	// TODO: fill transactions
	select {
	case ev := <-sub.Chan():
		block := ev.Data.(core.NewMinedBlockEvent).Block
		if _, err := chain.InsertChain([]*types.Block{block}); err != nil {
			t.Fatalf("failed to insert new mined block %d: %v", block.NumberU64(), err)
		}
	case <-time.After(10000 * time.Second): // Worker needs 1s to include new changes.
		t.Fatalf("timeout")
	}
}
