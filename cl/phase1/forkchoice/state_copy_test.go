// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package forkchoice

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
)

type currentStateForkGraph struct {
	fork_graph.ForkGraph
	root    common.Hash
	current *state.CachingBeaconState
	entered chan struct{}
	release chan struct{}
}

func (g *currentStateForkGraph) CurrentStateLocked(root common.Hash) *state.CachingBeaconState {
	if g.entered != nil {
		select {
		case g.entered <- struct{}{}:
		default:
		}
	}
	if g.release != nil {
		<-g.release
	}
	if root != g.root {
		return nil
	}
	return g.current
}

func (g *currentStateForkGraph) ReplayState(common.Hash) (*state.CachingBeaconState, error) {
	return nil, nil
}

func TestCopyCurrentStateLockedOwnsResult(t *testing.T) {
	root := common.Hash{0x01}
	current := state.New(&clparams.MainnetBeaconConfig)
	store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{root: root, current: current}}

	copied, found, err := store.copyCurrentStateLocked(root, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.NotSame(t, current, copied)

	otherConfig := clparams.MainnetBeaconConfig
	mismatched := state.New(&otherConfig)
	copied, found, err = store.copyCurrentStateLocked(root, mismatched)
	require.NoError(t, err)
	require.True(t, found)
	require.NotSame(t, mismatched, copied)
	require.Same(t, current.BeaconConfig(), copied.BeaconConfig())

	_, found, err = store.copyCurrentStateLocked(root, current)
	require.True(t, found)
	require.Error(t, err)
}

func TestCopyStateAtBlockRootHoldsReadLockDuringCurrentStateCopy(t *testing.T) {
	root := common.Hash{0x01}
	graph := &currentStateForkGraph{
		root:    root,
		current: state.New(&clparams.MainnetBeaconConfig),
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	store := &ForkChoiceStore{forkGraph: graph}
	copyDone := make(chan error, 1)
	go func() {
		_, err := store.copyStateAtBlockRoot(root, nil)
		copyDone <- err
	}()
	<-graph.entered

	writerDone := make(chan struct{})
	go func() {
		store.mu.Lock()
		close(writerDone)
		store.mu.Unlock()
	}()
	select {
	case <-writerDone:
		t.Fatal("writer entered while current state copy was in progress")
	case <-time.After(20 * time.Millisecond):
	}

	close(graph.release)
	require.NoError(t, <-copyDone)
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not enter after current state copy completed")
	}
}

func TestCopyStateAtBlockRootReleasesReadLockAfterPanic(t *testing.T) {
	root := common.Hash{0x01}
	current := state.New(&clparams.MainnetBeaconConfig)
	current.SetFork(nil)
	store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{root: root, current: current}}
	panicValue := make(chan any, 1)
	go func() {
		defer func() { panicValue <- recover() }()
		_, _ = store.copyStateAtBlockRoot(root, nil)
	}()
	require.NotNil(t, <-panicValue)

	writerDone := make(chan struct{})
	go func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		close(writerDone)
	}()
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writer remained blocked after state copy panicked")
	}
}

func TestCopyStateAtBlockRootLockedDoesNotRelock(t *testing.T) {
	root := common.Hash{0x01}
	store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{
		root:    root,
		current: state.New(&clparams.MainnetBeaconConfig),
	}}
	store.mu.Lock()
	locked := true
	defer func() {
		if locked {
			store.mu.Unlock()
		}
	}()

	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		store.mu.Lock()
		close(writerDone)
		store.mu.Unlock()
	}()
	<-writerStarted
	time.Sleep(20 * time.Millisecond)

	copyDone := make(chan error, 1)
	go func() {
		_, err := store.copyStateAtBlockRootLocked(root, nil)
		copyDone <- err
	}()
	select {
	case err := <-copyDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("locked state copy tried to acquire the fork-choice lock")
	}

	store.mu.Unlock()
	locked = false
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("queued writer did not complete")
	}
}

func TestStateCollectionReadersPreserveNotFoundError(t *testing.T) {
	store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{}}
	root := common.Hash{0xff}

	_, err := store.GetBalances(root)
	require.ErrorIs(t, err, fork_graph.ErrStateNotFound)
	_, err = store.GetInactivitiesScores(root)
	require.ErrorIs(t, err, fork_graph.ErrStateNotFound)
	_, err = store.GetValidatorSet(root)
	require.ErrorIs(t, err, fork_graph.ErrStateNotFound)
}

func TestStateCollectionReadersCopyOnlyRequestedCollection(t *testing.T) {
	root := common.Hash{0x01}

	t.Run("balances", func(t *testing.T) {
		current := state.New(&clparams.MainnetBeaconConfig)
		current.SetFork(nil)
		current.SetBalances(solid.NewUint64ListSSZFromSlice(2, []uint64{11}))
		store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{root: root, current: current}}

		balances, err := store.GetBalances(root)
		require.NoError(t, err)
		balances.Set(0, 22)
		require.Equal(t, uint64(11), current.Balances().Get(0))
	})

	t.Run("inactivity scores", func(t *testing.T) {
		current := state.New(&clparams.MainnetBeaconConfig)
		current.SetFork(nil)
		current.SetInactivityScores([]uint64{11})
		store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{root: root, current: current}}

		scores, err := store.GetInactivitiesScores(root)
		require.NoError(t, err)
		scores.Set(0, 22)
		require.Equal(t, uint64(11), current.InactivityScores().Get(0))
	})

	t.Run("validators", func(t *testing.T) {
		current := state.New(&clparams.MainnetBeaconConfig)
		current.SetFork(nil)
		validators := solid.NewValidatorSet(2)
		validator := solid.NewValidator()
		validator.SetEffectiveBalance(11)
		validators.Append(validator)
		current.SetValidators(validators)
		store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{root: root, current: current}}

		copied, err := store.GetValidatorSet(root)
		require.NoError(t, err)
		copied.SetEffectiveBalanceForValidatorAtIndex(0, 22)
		require.Equal(t, uint64(11), current.ValidatorSet().Get(0).EffectiveBalance())
	})
}

func TestStateCollectionReadersConcurrentWithHash(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	current := state.New(&cfg)
	current.SetVersion(clparams.AltairVersion)
	validators := solid.NewValidatorSet(2048)
	balances := make([]uint64, 2048)
	for i := range 2048 {
		validator := solid.NewValidator()
		validator.SetEffectiveBalance(uint64(i + 1))
		validators.Append(validator)
		balances[i] = uint64(i + 1)
	}
	current.SetValidators(validators)
	current.SetBalances(solid.NewUint64ListSSZFromSlice(int(cfg.ValidatorRegistryLimit), balances))
	current.SetInactivityScores(balances)
	root := common.Hash{0x01}
	store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{root: root, current: current}}
	start := make(chan struct{})
	errs := make(chan error, 4)
	var waitGroup sync.WaitGroup
	waitGroup.Go(func() {
		<-start
		errs <- store.ViewStateAtBlockRoot(root, func(st *state.CachingBeaconState) error {
			_, err := st.HashSSZ()
			return err
		})
	})
	waitGroup.Go(func() {
		<-start
		_, err := store.GetBalances(root)
		errs <- err
	})
	waitGroup.Go(func() {
		<-start
		_, err := store.GetInactivitiesScores(root)
		errs <- err
	})
	waitGroup.Go(func() {
		<-start
		_, err := store.GetValidatorSet(root)
		errs <- err
	})
	close(start)
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
