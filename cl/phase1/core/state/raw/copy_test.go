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

package raw

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestBeaconStateReadersWaitForStateLock(t *testing.T) {
	tests := map[string]func(*BeaconState) error{
		"copy": func(source *BeaconState) error {
			return source.CopyInto(New(source.BeaconConfig()))
		},
		"balances copy": func(source *BeaconState) error {
			source.CopyBalances()
			return nil
		},
		"inactivity scores copy": func(source *BeaconState) error {
			source.CopyInactivityScores()
			return nil
		},
		"validator set copy": func(source *BeaconState) error {
			source.CopyValidatorSet()
			return nil
		},
		"current sync committee branch": func(source *BeaconState) error {
			_, err := source.CurrentSyncCommitteeBranch()
			return err
		},
		"next sync committee branch": func(source *BeaconState) error {
			_, err := source.NextSyncCommitteeBranch()
			return err
		},
		"finality root branch": func(source *BeaconState) error {
			_, err := source.FinalityRootBranch()
			return err
		},
	}

	for name, read := range tests {
		t.Run(name, func(t *testing.T) {
			source := New(&clparams.MainnetBeaconConfig)
			source.mu.Lock()
			done := make(chan error, 1)
			go func() { done <- read(source) }()

			select {
			case err := <-done:
				t.Fatalf("reader completed while state lock was held: %v", err)
			case <-time.After(20 * time.Millisecond):
			}

			source.mu.Unlock()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("reader did not complete after state lock was released")
			}
		})
	}
}

func TestBeaconStateCopiesShareReadLockWhileHashWaits(t *testing.T) {
	source := New(&clparams.MainnetBeaconConfig)
	source.validators.Append(solid.NewValidator())
	_, err := source.validators.HashSSZ()
	require.NoError(t, err)

	firstTarget := New(source.BeaconConfig())
	firstRelease, firstTreeDone := holdValidatorTreeLock(t, firstTarget)
	var releaseFirst sync.Once
	defer releaseFirst.Do(func() { close(firstRelease) })

	firstCopyDone := make(chan error, 1)
	go func() { firstCopyDone <- source.CopyInto(firstTarget) }()
	waitForStateLock(t, source)

	secondTarget := New(source.BeaconConfig())
	secondRelease, secondTreeDone := holdValidatorTreeLock(t, secondTarget)
	secondCopyDone := make(chan error, 1)
	go func() { secondCopyDone <- source.CopyInto(secondTarget) }()
	waitForBlockedStateCopies(t, 2)

	hashDone := make(chan error, 1)
	go func() {
		_, err := source.HashSSZ()
		hashDone <- err
	}()
	waitForStateWriter(t, source, hashDone)

	releaseFirst.Do(func() { close(firstRelease) })
	require.NoError(t, <-firstTreeDone)
	require.NoError(t, <-firstCopyDone)
	if source.mu.TryRLock() {
		source.mu.RUnlock()
		t.Fatal("hash stopped waiting while the second copy held the source read lock")
	}

	close(secondRelease)
	require.NoError(t, <-secondTreeDone)
	require.NoError(t, <-secondCopyDone)
	select {
	case err := <-hashDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("hash did not complete after copies released the source read lock")
	}
}

func TestBeaconStateConcurrentCopies(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			clparams.ApplyMinimalPreset(&cfg)
			source := New(&cfg)
			source.SetVersion(version)
			currentCommittee := &solid.SyncCommittee{}
			currentCommittee.Bytes()
			nextCommittee := &solid.SyncCommittee{}
			nextCommittee.Bytes()
			require.NoError(t, source.SetCurrentSyncCommittee(currentCommittee))
			require.NoError(t, source.SetNextSyncCommittee(nextCommittee))

			const copyCount = 8
			start := make(chan struct{})
			targets := make([]*BeaconState, copyCount)
			errs := make(chan error, copyCount+1)
			var waitGroup sync.WaitGroup
			for i := range copyCount {
				waitGroup.Go(func() {
					<-start
					targets[i] = New(&cfg)
					errs <- source.CopyInto(targets[i])
				})
			}
			waitGroup.Go(func() {
				<-start
				_, err := source.HashSSZ()
				errs <- err
			})
			close(start)
			waitGroup.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}

			sourceRoot, err := source.HashSSZ()
			require.NoError(t, err)
			for _, target := range targets {
				targetRoot, err := target.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, sourceRoot, targetRoot)
			}
		})
	}
}

func holdValidatorTreeLock(t *testing.T, state *BeaconState) (chan struct{}, chan error) {
	t.Helper()
	state.validators.Append(solid.NewValidator())
	_, err := state.validators.HashSSZ()
	require.NoError(t, err)
	entered := make(chan struct{})
	release := make(chan struct{})
	state.validators.MerkleTree.SetComputeLeafFn(func(_ int, _ []byte) {
		close(entered)
		<-release
	})
	state.validators.MerkleTree.MarkLeafAsDirty(0)
	done := make(chan error, 1)
	go func() {
		_, err := state.validators.HashSSZ()
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("validator tree did not enter leaf computation")
	}
	return release, done
}

func waitForStateLock(t *testing.T, state *BeaconState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for state.mu.TryLock() {
		state.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("copy did not acquire the source state lock")
		}
		runtime.Gosched()
	}
}

func waitForStateWriter(t *testing.T, state *BeaconState, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for state.mu.TryRLock() {
		state.mu.RUnlock()
		select {
		case err := <-done:
			t.Fatalf("hash completed while a copy held the source read lock: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("hash did not wait for the source write lock")
		}
		runtime.Gosched()
	}
}

func waitForBlockedStateCopies(t *testing.T, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	stackSize := 64 << 10
	for {
		stack := make([]byte, stackSize)
		stackLength := runtime.Stack(stack, true)
		if stackLength == len(stack) {
			stackSize *= 2
			continue
		}
		blocked := 0
		for goroutine := range bytes.SplitSeq(stack[:stackLength], []byte("\n\n")) {
			if bytes.Contains(goroutine, []byte("raw.(*BeaconState).CopyInto")) &&
				bytes.Contains(goroutine, []byte("merkle_tree.(*MerkleTree).CopyInto")) {
				blocked++
			}
		}
		if blocked >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("found %d blocked state copies, want %d", blocked, count)
		}
		runtime.Gosched()
	}
}

func TestBeaconStateCopyIntoDeepCopiesPtcWindow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	source := New(&cfg)
	source.SetVersion(clparams.GloasVersion)
	source.GetPtcWindow().Get(0).Set(0, 11)
	target := New(&cfg)
	targetInner := target.GetPtcWindow().Get(0)
	sourceRoot, err := source.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	_, err = target.GetPtcWindow().HashSSZ()
	require.NoError(t, err)

	require.NoError(t, source.CopyInto(target))
	require.Same(t, targetInner, target.GetPtcWindow().Get(0))
	require.NotSame(t, source.GetPtcWindow().Get(0), target.GetPtcWindow().Get(0))
	targetRoot, err := target.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, sourceRoot, targetRoot)
	target.GetPtcWindow().Get(0).Set(0, 22)
	target.GetPtcWindow().Set(0, target.GetPtcWindow().Get(0))

	require.Equal(t, uint64(11), source.GetPtcWindow().Get(0).Get(0))
	require.Equal(t, uint64(22), target.GetPtcWindow().Get(0).Get(0))
	unchangedSourceRoot, err := source.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, sourceRoot, unchangedSourceRoot)
	mutatedTargetRoot, err := target.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	source.GetPtcWindow().Get(0).Set(0, 33)
	source.GetPtcWindow().Set(0, source.GetPtcWindow().Get(0))
	unchangedTargetRoot, err := target.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, mutatedTargetRoot, unchangedTargetRoot)
}

func TestBeaconStateCopyIntoPtcWindowConcurrentFirstHash(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	source := New(&cfg)
	source.SetVersion(clparams.GloasVersion)
	target := New(&cfg)
	require.NoError(t, source.CopyInto(target))

	var waitGroup sync.WaitGroup
	errs := make(chan error, 2)
	for _, state := range []*BeaconState{source, target} {
		waitGroup.Go(func() {
			_, err := state.GetPtcWindow().HashSSZ()
			errs <- err
		})
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
