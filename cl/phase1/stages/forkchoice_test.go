package stages

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type pausingHeadView struct {
	synced_data.SyncedData
	entered chan struct{}
	release chan struct{}
}

type unpublishedHeadState struct {
	synced_data.SyncedData
}

func (s *unpublishedHeadState) OnHeadStateFrom(common.Hash, func(common.Hash, *state.CachingBeaconState) (*state.CachingBeaconState, error)) (bool, error) {
	return false, nil
}

type mismatchedHeadIdentity struct {
	synced_data.SyncedData
}

func (s *mismatchedHeadIdentity) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	return s.SyncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, _ common.Hash, slot uint64) error {
		return fn(headState, common.Hash{0xff}, slot)
	})
}

type failingWriteFs struct {
	afero.Fs
	fail atomic.Bool
}

type failingHighestFinalizedTx struct {
	kv.RwTx
}

var mismatchPreCacheRoot atomic.Uint32

func (tx *failingHighestFinalizedTx) Put(table string, key, value []byte) error {
	if table == kv.HighestFinalized && bytes.Equal(key, kv.HighestFinalizedKey) {
		return errors.New("highest finalized write failed")
	}
	return tx.RwTx.Put(table, key, value)
}

func (f *failingWriteFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if f.fail.Load() && flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		return nil, errors.New("write failed")
	}
	return f.Fs.OpenFile(name, flag, perm)
}

func (s *pausingHeadView) ViewHeadState(fn synced_data.ViewHeadStateFn) error {
	return s.SyncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		close(s.entered)
		<-s.release
		return fn(headState)
	})
}

func (s *pausingHeadView) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	return s.SyncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, slot uint64) error {
		close(s.entered)
		<-s.release
		return fn(headState, root, slot)
	})
}

type statusSentinel struct {
	sentinelproto.SentinelClient
}

func (*statusSentinel) SetStatus(context.Context, *sentinelproto.Status, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	return &sentinelproto.EmptyMessage{}, nil
}

func (*statusSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

type noOpAttestationDataProducer struct {
	attestation_producer.AttestationDataProducer
}

type recordingAttestationDataProducer struct {
	attestation_producer.AttestationDataProducer
	state *state.CachingBeaconState
	root  common.Hash
}

func (p *recordingAttestationDataProducer) ProduceAndCacheAttestationData(_ kv.Tx, st *state.CachingBeaconState, root common.Hash, _ uint64) (solid.AttestationData, error) {
	p.state = st
	p.root = root
	return solid.AttestationData{}, nil
}

func (noOpAttestationDataProducer) ProduceAndCacheAttestationData(kv.Tx, *state.CachingBeaconState, common.Hash, uint64) (solid.AttestationData, error) {
	return solid.AttestationData{}, nil
}

type lockOrderFixture struct {
	cfg        *Cfg
	syncedData *synced_data.SyncedDataManager
	headState  *state.CachingBeaconState
	headRoot   common.Hash
	view       *pausingHeadView
}

func newLockOrderFixture(t *testing.T, slot uint64) lockOrderFixture {
	t.Helper()
	return newLockOrderFixtureWithFS(t, slot, afero.NewMemMapFs())
}

func newLockOrderFixtureWithFS(t *testing.T, slot uint64, fs afero.Fs) lockOrderFixture {
	t.Helper()
	return newLockOrderFixtureWithFSAndBlockRoot(t, slot, fs, common.Hash{})
}

func newLockOrderFixtureWithFSAndBlockRoot(t *testing.T, slot uint64, fs afero.Fs, blockRoot common.Hash) lockOrderFixture {
	t.Helper()
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.GloasForkEpoch = math.MaxUint64
	genesisTime := uint64(time.Now().Unix()) - slot*beaconCfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, &beaconCfg)
	headState := state.New(&beaconCfg)
	headState.SetGenesisTime(genesisTime)
	require.NoError(t, headState.SetSlot(slot))
	if blockRoot != (common.Hash{}) {
		epoch := state.Epoch(headState) + 1
		refSlot := (epoch-1)*beaconCfg.SlotsPerEpoch - 1
		require.NoError(t, headState.SetBlockRootAt(int(refSlot%beaconCfg.SlotsPerHistoricalRoot), blockRoot))
	}
	headState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: slot})
	validator := solid.NewValidatorFromParameters(
		common.Bytes48{}, common.Hash{}, beaconCfg.MaxEffectiveBalance, false,
		0, 0, beaconCfg.FarFutureEpoch, beaconCfg.FarFutureEpoch,
	)
	require.NoError(t, headState.AddValidator(validator, beaconCfg.MaxEffectiveBalance))

	manager := synced_data.NewSyncedDataManager(&beaconCfg, true)
	emitter := beaconevents.NewEventEmitter()
	graph, err := fork_graph.NewForkGraphDisk(
		headState,
		manager,
		fs,
		beacon_router_configuration.RouterConfiguration{},
	)
	require.NoError(t, err)
	store, err := forkchoice.NewForkChoiceStore(
		clock,
		headState,
		nil,
		pool.NewOperationsPool(&beaconCfg),
		graph,
		emitter,
		manager,
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)
	headRoot := graph.AnchorRoot()
	require.NoError(t, manager.OnHeadStateWithBlockRoot(headState, headRoot))

	viewEntered := make(chan struct{})
	releaseView := make(chan struct{})
	pausedView := &pausingHeadView{
		SyncedData: manager,
		entered:    viewEntered,
		release:    releaseView,
	}
	return lockOrderFixture{
		cfg: &Cfg{
			rpc:        rpc.NewBeaconRpcP2P(t.Context(), &statusSentinel{}, &beaconCfg, clock),
			ethClock:   clock,
			beaconCfg:  &beaconCfg,
			forkChoice: store,
			syncedData: pausedView,
			emitter:    emitter,
		},
		syncedData: manager,
		headState:  headState,
		headRoot:   headRoot,
		view:       pausedView,
	}
}

func requireLockOrderOperationCompletes(t *testing.T, fixture lockOrderFixture, operationDone <-chan error) {
	t.Helper()
	select {
	case err := <-operationDone:
		// An operation that never takes the synced-data lock cannot invert the order.
		require.NoError(t, err)
		return
	case <-fixture.view.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("operation neither completed nor entered the synced head-state view")
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- fixture.syncedData.OnHeadStateWithBlockRoot(fixture.headState, fixture.headRoot)
	}()
	time.Sleep(250 * time.Millisecond)

	slashingDone := make(chan error, 1)
	go func() {
		slashingDone <- fixture.cfg.forkChoice.OnAttesterSlashing(cltypes.NewAttesterSlashing(clparams.Phase0Version), true)
	}()
	select {
	case err := <-slashingDone:
		t.Fatalf("lock-order setup failed: attester slashing completed before the queued writer: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	close(fixture.view.release)
	select {
	case err := <-operationDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("operation deadlocked: synced-data read lock waited for fork choice while fork choice waited behind a queued synced-data writer")
	}
	require.NoError(t, <-writerDone)
	<-slashingDone
}

func TestComputeForkChoiceDoesNotInvertSyncedDataLockOrder(t *testing.T) {
	fixture := newLockOrderFixture(t, 0)
	done := make(chan error, 1)
	go func() {
		_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), fixture.cfg)
		done <- err
	}()

	requireLockOrderOperationCompletes(t, fixture, done)
}

func TestComputeForkChoiceFallsBackWithoutSyncedHeadState(t *testing.T) {
	fixture := newLockOrderFixture(t, 0)
	fixture.syncedData.UnsetHeadState()

	headSlot, headRoot, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), fixture.cfg)

	require.NoError(t, err)
	require.Equal(t, fixture.headState.Slot(), headSlot)
	require.Equal(t, fixture.headRoot, headRoot)
}

func TestPostForkchoiceOperationsDoesNotInvertSyncedDataLockOrder(t *testing.T) {
	fixture := newLockOrderFixture(t, clparams.MainnetBeaconConfig.SlotsPerEpoch*5)
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = noOpAttestationDataProducer{}
	fixture.cfg.dirs = datadir.New(t.TempDir())
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	done := make(chan error, 1)
	go func() {
		done <- postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot)
	}()

	requireLockOrderOperationCompletes(t, fixture, done)
	_, err = checkpoint_sync.ReadLocalFinalizedState(fixture.cfg.dirs, fixture.cfg.beaconCfg)
	require.NoError(t, err)
}

func TestPostForkchoiceOperationsUsesPublishedSyncedHead(t *testing.T) {
	fixture := newLockOrderFixture(t, 1)
	producer := &recordingAttestationDataProducer{}
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = producer
	fixture.cfg.syncedData = fixture.syncedData
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	require.NoError(t, postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot))
	require.Equal(t, fixture.headRoot, producer.root)
	require.NoError(t, fixture.syncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, slot uint64) error {
		require.Same(t, producer.state, headState)
		require.Equal(t, fixture.headRoot, root)
		require.Equal(t, fixture.headState.Slot(), slot)
		return nil
	}))
}

func TestPostForkchoiceOperationsPublishesWhenHeadMoved(t *testing.T) {
	fixture := newLockOrderFixture(t, 1)
	producer := &recordingAttestationDataProducer{}
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = producer
	fixture.cfg.syncedData = fixture.syncedData
	events := make(chan *beaconevents.EventStream, 4)
	subscription := fixture.cfg.emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	var previousHead *state.CachingBeaconState
	require.NoError(t, fixture.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		previousHead = headState
		return nil
	}))

	require.NoError(t, postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot()+1, fixture.headRoot))
	require.Equal(t, fixture.headRoot, producer.root)
	require.NoError(t, fixture.syncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, _ uint64) error {
		require.NotSame(t, previousHead, headState)
		require.Same(t, producer.state, headState)
		require.Equal(t, fixture.headRoot, root)
		return nil
	}))
	for len(events) > 0 {
		event := <-events
		require.NotEqual(t, beaconevents.StateHead, event.Event)
		require.NotEqual(t, beaconevents.StateHeadV2, event.Event)
	}
}

func TestPostForkchoiceOperationsWritesFinalizationButDoesNotSetHeadBeforePublish(t *testing.T) {
	fixture := newLockOrderFixture(t, 1)
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = noOpAttestationDataProducer{}
	fixture.cfg.syncedData = &unpublishedHeadState{SyncedData: fixture.syncedData}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	require.NoError(t, beacon_indicies.WriteHighestFinalized(tx, 99))
	finalizedSlot := fixture.cfg.forkChoice.FinalizedSlot()

	require.NoError(t, postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot))
	require.Zero(t, fixture.cfg.blobDownloader.HeadSlot())
	require.False(t, fixture.cfg.forkChoice.Synced())
	highestFinalized, err := beacon_indicies.ReadHighestFinalized(tx)
	require.NoError(t, err)
	require.Equal(t, finalizedSlot, highestFinalized)
}

func TestPostForkchoiceOperationsDoesNotPublishBeforeHighestFinalizedWrite(t *testing.T) {
	fixture := newLockOrderFixture(t, 1)
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = noOpAttestationDataProducer{}
	fixture.cfg.syncedData = fixture.syncedData
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	var previousHead *state.CachingBeaconState
	require.NoError(t, fixture.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		previousHead = headState
		return nil
	}))

	err = postForkchoiceOperations(&failingHighestFinalizedTx{RwTx: tx}, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot)
	require.ErrorContains(t, err, "highest finalized write failed")
	require.NoError(t, fixture.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Same(t, previousHead, headState)
		return nil
	}))
}

func TestPostForkchoiceOperationsMaintainsPublishedStateAfterIdentityChanges(t *testing.T) {
	fs := afero.NewMemMapFs()
	var preCacheRoot common.Hash
	binary.BigEndian.PutUint32(preCacheRoot[:], mismatchPreCacheRoot.Add(1))
	fixture := newLockOrderFixtureWithFSAndBlockRoot(t, clparams.MainnetBeaconConfig.SlotsPerEpoch*12_345, fs, preCacheRoot)
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	producer := &recordingAttestationDataProducer{}
	fixture.cfg.attestationDataProducer = producer
	fixture.cfg.syncedData = &mismatchedHeadIdentity{SyncedData: fixture.syncedData}
	fixture.cfg.dirs = datadir.New(t.TempDir())
	stateFilename := fmt.Sprintf("%x.snappy_ssz", fixture.headRoot)
	require.NoError(t, fs.Remove(stateFilename))
	require.Eventually(t, func() bool { return !workingPreCacheNextShuffledValidatorSet.Load() }, 10*time.Second, time.Millisecond)
	epoch := state.Epoch(fixture.headState) + 1
	refSlot := (epoch-1)*fixture.cfg.beaconCfg.SlotsPerEpoch - 1
	blockRoot, err := fixture.headState.GetBlockRootAtSlot(refSlot)
	require.NoError(t, err)
	_, cached := caches.ShuffledIndiciesCacheGlobal.Get(epoch, blockRoot)
	require.False(t, cached)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	require.NoError(t, postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot))
	_, err = checkpoint_sync.ReadLocalFinalizedState(fixture.cfg.dirs, fixture.cfg.beaconCfg)
	require.NoError(t, err)
	exists, err := afero.Exists(fs, stateFilename)
	require.NoError(t, err)
	require.True(t, exists)
	require.Nil(t, producer.state)
	require.Eventually(t, func() bool {
		_, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, blockRoot)
		return ok
	}, 10*time.Second, time.Millisecond)
}

func TestPostForkchoiceOperationsEmitsEventsBeforeReturningDumpError(t *testing.T) {
	fs := &failingWriteFs{Fs: afero.NewMemMapFs()}
	fixture := newLockOrderFixtureWithFS(t, 4, fs)
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = noOpAttestationDataProducer{}
	fixture.cfg.syncedData = fixture.syncedData
	events := make(chan *beaconevents.EventStream, 4)
	subscription := fixture.cfg.emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	fs.fail.Store(true)

	err = postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot)
	require.ErrorContains(t, err, "failed to dump beacon state on disk")
	seen := map[beaconevents.EventTopic]bool{}
	for len(events) > 0 {
		event := <-events
		seen[event.Event] = true
	}
	require.True(t, seen[beaconevents.StateHead])
	require.True(t, seen[beaconevents.StateHeadV2])
	require.True(t, seen[beaconevents.StatePayloadAttributes])
}

func TestPayloadAttributesUseSepoliaGasLimitScheduleAtGloas(t *testing.T) {
	_, config := clparams.GetConfigsByNetwork(chainspec.SepoliaChainID)
	headSlot := config.GloasForkEpoch*config.SlotsPerEpoch - 1
	headState := state.New(config)
	headState.SetVersion(clparams.FuluVersion)
	require.NoError(t, headState.SetSlot(headSlot))
	emitter := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	cfg := &Cfg{
		beaconCfg: config,
		ethClock:  eth_clock.NewEthereumClock(0, common.Hash{}, config),
		emitter:   emitter,
	}

	event, err := buildNextPayloadAttributesEvent(cfg, headSlot, common.Hash{}, headState)
	require.NoError(t, err)
	require.NotNil(t, event)
	cfg.emitter.State().SendPayloadAttributes(event)

	require.Len(t, events, 1)
	attrs := (<-events).Data.(*beaconevents.PayloadAttributesData).Data.PayloadAttributes
	require.NotNil(t, attrs.TargetGasLimit)
	require.Equal(t, hexutil.Uint64(200_000_000), *attrs.TargetGasLimit)
}

func TestUpdateCanonicalChainReorgEvent(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}

	state100 := common.Hash{0xa0}
	state101a := common.Hash{0xa1}
	state102a := common.Hash{0xa2}
	state101b := common.Hash{0xb1}
	state102b := common.Hash{0xb2}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, state100, 100, true)
	writeBlock(root101a, root100, state101a, 101, true)
	writeBlock(root102a, root101a, state102a, 102, true)

	writeBlock(root101b, root100, state101b, 101, false)
	writeBlock(root102b, root101b, state102b, 102, false)

	reorg := drainReorgEvent(t, tx, 102, root102b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(102), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(2), reorg.Depth, "reorg Depth should be oldHeadSlot - forkPointSlot")
	require.Equal(t, root102a, reorg.OldHeadBlock, "OldHeadBlock should be the previous canonical tip")
	require.Equal(t, root102b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state102a, reorg.OldHeadState, "OldHeadState should match old canonical tip's state root")
	require.Equal(t, state102b, reorg.NewHeadState, "NewHeadState")
}

func TestUpdateCanonicalChainReorgShorterFork(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root103a := common.Hash{0x13, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}

	state100 := common.Hash{0xa0}
	state103a := common.Hash{0xa3}
	state102b := common.Hash{0xb2}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, state100, 100, true)
	writeBlock(root101a, root100, common.Hash{0xa1}, 101, true)
	writeBlock(root102a, root101a, common.Hash{0xa2}, 102, true)
	writeBlock(root103a, root102a, state103a, 103, true)

	writeBlock(root101b, root100, common.Hash{0xb1}, 101, false)
	writeBlock(root102b, root101b, state102b, 102, false)

	reorg := drainReorgEvent(t, tx, 102, root102b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(102), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(3), reorg.Depth, "reorg Depth: old tip 103 - fork point 100 = 3")
	require.Equal(t, root103a, reorg.OldHeadBlock, "OldHeadBlock must be the actual old tip at slot 103, not 102a")
	require.Equal(t, root102b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state103a, reorg.OldHeadState, "OldHeadState must match old tip's state root")
	require.Equal(t, state102b, reorg.NewHeadState, "NewHeadState")
}

func TestUpdateCanonicalChainReorgLongerFork(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}
	root103b := common.Hash{0x13, 0xbb}

	state102a := common.Hash{0xa2}
	state103b := common.Hash{0xb3}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101a, root100, common.Hash{0xa1}, 101, true)
	writeBlock(root102a, root101a, state102a, 102, true)

	writeBlock(root101b, root100, common.Hash{0xb1}, 101, false)
	writeBlock(root102b, root101b, common.Hash{0xb2}, 102, false)
	writeBlock(root103b, root102b, state103b, 103, false)

	reorg := drainReorgEvent(t, tx, 103, root103b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(103), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(2), reorg.Depth, "reorg Depth: old tip 102 - fork point 100 = 2")
	require.Equal(t, root102a, reorg.OldHeadBlock, "OldHeadBlock must be the old tip at slot 102")
	require.Equal(t, root103b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state102a, reorg.OldHeadState, "OldHeadState")
	require.Equal(t, state103b, reorg.NewHeadState, "NewHeadState")
}

func TestUpdateCanonicalChainNoReorg(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101 := common.Hash{0x11}
	root102 := common.Hash{0x12}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101, root100, common.Hash{0xa1}, 101, true)

	writeBlock(root102, root101, common.Hash{0xa2}, 102, false)

	reorg := drainReorgEvent(t, tx, 102, root102)
	require.Nil(t, reorg, "chain extension should NOT emit a chain_reorg event")
}

func TestUpdateCanonicalChainReorgOneSlot(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root101b := common.Hash{0x11, 0xbb}

	state101a := common.Hash{0xa1}
	state101b := common.Hash{0xb1}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101a, root100, state101a, 101, true)
	writeBlock(root101b, root100, state101b, 101, false)

	reorg := drainReorgEvent(t, tx, 101, root101b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(101), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(1), reorg.Depth, "reorg Depth: old tip 101 - fork point 100 = 1")
	require.Equal(t, root101a, reorg.OldHeadBlock, "OldHeadBlock must be root_101a")
	require.Equal(t, root101b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state101a, reorg.OldHeadState, "OldHeadState")
	require.Equal(t, state101b, reorg.NewHeadState, "NewHeadState")
}

func drainReorgEvent(t *testing.T, tx kv.RwTx, headSlot uint64, headRoot common.Hash) *beaconevents.ChainReorgData {
	t.Helper()
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 16)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()

	cfg := &Cfg{
		emitter:   emitter,
		beaconCfg: &clparams.MainnetBeaconConfig,
	}

	err := updateCanonicalChainInTheDatabase(tx, headSlot, headRoot, cfg)
	require.NoError(t, err)

	for {
		select {
		case evt := <-ch:
			if evt.Event == beaconevents.StateChainReorg {
				return evt.Data.(*beaconevents.ChainReorgData)
			}
		default:
			return nil
		}
	}
}

type invalidatingEventHeadStore struct {
	*mock_services.ForkChoiceStorageMock
	reads           int
	invalidateAfter int
}

func (s *invalidatingEventHeadStore) readHead() (forkchoice.ForkChoiceNode, uint64, error) {
	s.reads++
	s.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	head := forkchoice.ForkChoiceNode{Root: s.HeadVal, PayloadStatus: s.HeadPayloadStatusVal}
	// Invalidation after a head read must not change the status returned with that root.
	if s.reads == s.invalidateAfter {
		s.HeadPayloadStatusVal = cltypes.PayloadStatusPending
	}
	return head, s.HeadSlotVal, nil
}

func (s *invalidatingEventHeadStore) GetHead(*state.CachingBeaconState) (common.Hash, uint64, error) {
	head, slot, err := s.readHead()
	return head.Root, slot, err
}

func (s *invalidatingEventHeadStore) GetHeadNode() (forkchoice.ForkChoiceNode, uint64, error) {
	return s.readHead()
}

func TestEmitHeadEventKeepsPayloadStatusWithHeadSnapshot(t *testing.T) {
	for _, test := range []struct {
		name            string
		invalidateAfter int
	}{
		{name: "initial snapshot", invalidateAfter: 1},
		{name: "publication recheck", invalidateAfter: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			beaconCfg := clparams.MainnetBeaconConfig
			headState := state.New(&beaconCfg)
			headState.SetVersion(clparams.GloasVersion)
			require.NoError(t, headState.SetSlot(1))
			require.NoError(t, headState.SetBlockRootAt(0, common.Hash{1}))
			store := &invalidatingEventHeadStore{
				ForkChoiceStorageMock: &mock_services.ForkChoiceStorageMock{
					HeadVal: common.Hash{2}, HeadSlotVal: 1,
				},
				invalidateAfter: test.invalidateAfter,
			}
			emitter := beaconevents.NewEventEmitter()
			events := make(chan *beaconevents.EventStream, 2)
			subscription := emitter.State().Subscribe(events)
			defer subscription.Unsubscribe()
			head, slot, err := store.GetHeadNode()
			require.NoError(t, err)
			headEvent, stateRoot, err := buildHeadEvent(&beaconCfg, store.HeadSlotVal, store.HeadVal, headState, head, store.IsRootOptimistic(store.HeadVal))
			require.NoError(t, err)
			require.NoError(t, emitHeadEventsIfCurrent(emitter, headEvent, slot, head.Root, stateRoot, headEventRevalidator(store, true)))

			require.Equal(t, 2, store.reads)
			require.Len(t, events, 2, "cache invalidation must not drop a matching FULL head event")
			require.Equal(t, beaconevents.StateHead, (<-events).Event)
			v2 := <-events
			require.Equal(t, beaconevents.StateHeadV2, v2.Event)
			require.Equal(t, "full", v2.Data.(*beaconevents.HeadV2Data).Data.PayloadStatus)
		})
	}
}

func TestEmitHeadEventsDropsStaleSnapshot(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}

	err := emitHeadEventsIfCurrent(emitter, &beaconevents.HeadV2Data{}, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return common.Hash{3}, 11, "pending", false, nil
	})
	require.NoError(t, err)
	require.Empty(t, ch)
}

func TestEmitHeadEventsDropsStalePayloadStatus(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{PayloadStatus: "pending"}}

	err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "full", false, nil
	})
	require.NoError(t, err)
	require.Empty(t, ch)
}

func TestEmitHeadEventsDoesNotHoldHeadEventLockWhileLegacySubscriberBlocks(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	legacyEvents := make(chan *beaconevents.EventStream)
	sub := emitter.State().Subscribe(legacyEvents)
	defer sub.Unsubscribe()

	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{PayloadStatus: "pending"}}
	validated := make(chan struct{})
	emitDone := make(chan error, 1)
	go func() {
		emitDone <- emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
			select {
			case <-validated:
			default:
				close(validated)
			}
			return headRoot, 10, "pending", false, nil
		})
	}()
	<-validated

	lockAcquired := make(chan struct{})
	go emitter.WithHeadEventLock(func() { close(lockAcquired) })
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("slow legacy subscriber held the shared head event lock")
	}

	sub.Unsubscribe()
	require.NoError(t, <-emitDone)
}

func TestEmitHeadEventsKeepsPairOrderedAgainstConcurrentUpdate(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	fastEvents := make(chan *beaconevents.EventStream, 3)
	slowEvents := make(chan *beaconevents.EventStream)
	fastSub := emitter.State().Subscribe(fastEvents)
	defer fastSub.Unsubscribe()
	slowSub := emitter.State().Subscribe(slowEvents)
	defer slowSub.Unsubscribe()

	headRoot := common.Hash{1}
	newHeadRoot := common.Hash{2}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{
		Slot:          10,
		Block:         headRoot,
		PayloadStatus: "pending",
	}}
	newHeadEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{
		Slot:          11,
		Block:         newHeadRoot,
		PayloadStatus: "full",
	}}
	validated := make(chan struct{})
	var validationCalls atomic.Int32
	var updated atomic.Bool
	emitDone := make(chan error, 1)
	go func() {
		emitDone <- emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{3}, func() (common.Hash, uint64, string, bool, error) {
			if validationCalls.Add(1) == 1 {
				close(validated)
			}
			if updated.Load() {
				return newHeadRoot, 11, "full", false, nil
			}
			return headRoot, 10, "pending", false, nil
		})
	}()
	<-validated

	first := <-fastEvents
	require.Equal(t, beaconevents.StateHead, first.Event)
	require.Equal(t, headRoot, first.Data.(*beaconevents.HeadData).Block)

	updated.Store(true)
	updateDone := make(chan struct{})
	go emitter.WithHeadEventLock(func() {
		emitter.State().SendHeadV2(newHeadEvent)
		close(updateDone)
	})
	select {
	case <-updateDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent head update did not complete")
	}

	second := <-fastEvents
	require.Equal(t, beaconevents.StateHeadV2, second.Event)
	require.Equal(t, headRoot, second.Data.(*beaconevents.HeadV2Data).Data.Block)

	slowSub.Unsubscribe()
	require.NoError(t, <-emitDone)
	third := <-fastEvents
	require.Equal(t, beaconevents.StateHeadV2, third.Event)
	require.Equal(t, newHeadRoot, third.Data.(*beaconevents.HeadV2Data).Data.Block)
}

func TestEmitHeadEventsPreservesLegacyHeadFields(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{
		PayloadStatus:             "pending",
		EpochTransition:           false,
		CurrentEpochDependentRoot: common.Hash{3},
		NextEpochDependentRoot:    common.Hash{4},
		ExecutionOptimistic:       true,
	}}

	err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "pending", true, nil
	})
	require.NoError(t, err)
	legacy := (<-ch).Data.(*beaconevents.HeadData)
	require.False(t, legacy.EpochTransition)
	require.True(t, legacy.ExecutionOptimistic)
	require.Equal(t, headEvent.Data.CurrentEpochDependentRoot, legacy.PreviousDutyDependentRoot)
	require.Equal(t, headEvent.Data.NextEpochDependentRoot, legacy.CurrentDutyDependentRoot)
}

func TestEmitHeadEventsPreGloasEmitsHeadV2AsFull(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{PayloadStatus: "full"}}

	err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "full", false, nil
	})
	require.NoError(t, err)
	require.Equal(t, beaconevents.StateHead, (<-ch).Event)
	select {
	case v2 := <-ch:
		require.Equal(t, beaconevents.StateHeadV2, v2.Event)
		require.Equal(t, "full", v2.Data.(*beaconevents.HeadV2Data).Data.PayloadStatus)
	case <-time.After(time.Second):
		t.Fatal("head_v2 event was not emitted")
	}
}
