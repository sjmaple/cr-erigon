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

package state

import (
	"encoding/binary"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestNewStateSetValidatorsDefersPubkeyIndex(t *testing.T) {
	beaconState := New(&clparams.MainnetBeaconConfig)
	validators := validatorSet(40, 41, 42)
	require.NotNil(t, beaconState.publicKeyIndices.base.entries)

	beaconState.SetValidators(validators)

	require.Nil(t, beaconState.publicKeyIndices.base.entries)
	for index := range validators.Length() {
		got, ok := beaconState.ValidatorIndexByPubkey(validators.Get(index).PublicKey())
		require.True(t, ok)
		require.Equal(t, uint64(index), got)
	}
	_, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(100).PublicKey())
	require.False(t, ok)
	require.NotNil(t, beaconState.publicKeyIndices.base.entries)
}

func TestDeferredPubkeyIndexConcurrentFirstLookup(t *testing.T) {
	source := New(&clparams.MainnetBeaconConfig)
	source.SetValidators(validatorSet(1, 2, 3, 4))
	states := []*CachingBeaconState{source}
	for range 3 {
		copied := New(source.BeaconConfig())
		require.NoError(t, source.CopyInto(copied))
		states = append(states, copied)
	}

	for _, beaconState := range states {
		require.Same(t, source.publicKeyIndices.base, beaconState.publicKeyIndices.base)
		require.Nil(t, beaconState.publicKeyIndices.base.entries)
	}

	start := make(chan struct{})
	var ready, waitGroup sync.WaitGroup
	ready.Add(16 * len(states))
	for round := range 16 {
		for _, beaconState := range states {
			waitGroup.Go(func() {
				ready.Done()
				<-start
				for index := range uint64(4) {
					got, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(index + 1).PublicKey())
					if !ok || got != index {
						t.Errorf("round %d lookup %d returned (%d, %v)", round, index+1, got, ok)
						return
					}
				}
			})
		}
	}
	ready.Wait()
	close(start)
	waitGroup.Wait()

	require.NotNil(t, source.publicKeyIndices.base.entries)
	entriesIdentity := reflect.ValueOf(source.publicKeyIndices.base.entries).Pointer()
	for _, beaconState := range states {
		require.Same(t, source.publicKeyIndices.base, beaconState.publicKeyIndices.base)
		_, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(1).PublicKey())
		require.True(t, ok)
		require.Equal(t, entriesIdentity, reflect.ValueOf(beaconState.publicKeyIndices.base.entries).Pointer())
	}
}

func TestDeferredPubkeyIndexCopyBuildsSharedBase(t *testing.T) {
	source := New(&clparams.MainnetBeaconConfig)
	source.SetValidators(validatorSet(1, 2, 3))
	copied := New(source.BeaconConfig())
	require.NoError(t, source.CopyInto(copied))
	require.Same(t, source.publicKeyIndices.base, copied.publicKeyIndices.base)
	require.Nil(t, copied.publicKeyIndices.base.entries)

	index, ok := copied.ValidatorIndexByPubkey(activeValidatorWithIndex(2).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(1), index)
	require.NotNil(t, source.publicKeyIndices.base.entries)
	require.Same(t, source.publicKeyIndices.base, copied.publicKeyIndices.base)
	entriesIdentity := reflect.ValueOf(source.publicKeyIndices.base.entries).Pointer()

	index, ok = source.ValidatorIndexByPubkey(activeValidatorWithIndex(3).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(2), index)
	require.Equal(t, entriesIdentity, reflect.ValueOf(source.publicKeyIndices.base.entries).Pointer())
}

func TestDecodedPubkeyIndexConcurrentFirstLookup(t *testing.T) {
	encoded, err := stateWithValidators(t, 1, 2, 3, 4).EncodeSSZ(nil)
	require.NoError(t, err)
	source := New(&clparams.MainnetBeaconConfig)
	require.NoError(t, source.DecodeSSZ(encoded, int(clparams.Phase0Version)))
	states := []*CachingBeaconState{source}
	for range 3 {
		copied := New(source.BeaconConfig())
		require.NoError(t, source.CopyInto(copied))
		states = append(states, copied)
	}

	for _, beaconState := range states {
		require.Same(t, source.publicKeyIndices.base, beaconState.publicKeyIndices.base)
		require.Nil(t, beaconState.publicKeyIndices.base.entries)
	}

	start := make(chan struct{})
	var ready, waitGroup sync.WaitGroup
	ready.Add(16 * len(states))
	for round := range 16 {
		for _, beaconState := range states {
			waitGroup.Go(func() {
				ready.Done()
				<-start
				for index := range uint64(4) {
					got, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(index + 1).PublicKey())
					if !ok || got != index {
						t.Errorf("round %d lookup %d returned (%d, %v)", round, index+1, got, ok)
						return
					}
				}
			})
		}
	}
	ready.Wait()
	close(start)
	waitGroup.Wait()

	require.NotNil(t, source.publicKeyIndices.base.entries)
	entriesIdentity := reflect.ValueOf(source.publicKeyIndices.base.entries).Pointer()
	for _, beaconState := range states {
		require.Same(t, source.publicKeyIndices.base, beaconState.publicKeyIndices.base)
		_, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(1).PublicKey())
		require.True(t, ok)
		require.Equal(t, entriesIdentity, reflect.ValueOf(beaconState.publicKeyIndices.base.entries).Pointer())
	}
}

func TestDecodedPubkeyIndexCopyIntoReusedStateBuildsSharedBase(t *testing.T) {
	encoded, err := stateWithValidators(t, 1, 2, 3).EncodeSSZ(nil)
	require.NoError(t, err)
	source := New(&clparams.MainnetBeaconConfig)
	require.NoError(t, source.DecodeSSZ(encoded, int(clparams.Phase0Version)))
	copied := stateWithValidators(t, 100, 101)
	require.NoError(t, source.CopyInto(copied))
	require.Same(t, source.publicKeyIndices.base, copied.publicKeyIndices.base)
	require.Nil(t, copied.publicKeyIndices.base.entries)

	index, ok := copied.ValidatorIndexByPubkey(activeValidatorWithIndex(2).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(1), index)
	require.NotNil(t, source.publicKeyIndices.base.entries)
	require.Same(t, source.publicKeyIndices.base, copied.publicKeyIndices.base)
	entriesIdentity := reflect.ValueOf(source.publicKeyIndices.base.entries).Pointer()

	index, ok = source.ValidatorIndexByPubkey(activeValidatorWithIndex(3).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(2), index)
	require.Equal(t, entriesIdentity, reflect.ValueOf(source.publicKeyIndices.base.entries).Pointer())
}

func TestDeferredPubkeyIndexCopyWithDeltaIntoReusedDestination(t *testing.T) {
	source := New(&clparams.MainnetBeaconConfig)
	source.SetValidators(validatorSet(1, 2))
	source.AppendValidator(activeValidatorWithIndex(3))
	copied := New(source.BeaconConfig())
	require.NoError(t, source.CopyInto(copied))
	sibling := New(source.BeaconConfig())
	require.NoError(t, source.CopyInto(sibling))

	require.Same(t, source.publicKeyIndices.base, copied.publicKeyIndices.base)
	require.Same(t, source.publicKeyIndices.base, sibling.publicKeyIndices.base)
	require.Nil(t, source.publicKeyIndices.base.entries)
	require.Len(t, copied.publicKeyIndices.delta, 1)

	copied.AppendValidator(activeValidatorWithIndex(4))
	require.Nil(t, source.publicKeyIndices.base.entries)
	index, ok := copied.ValidatorIndexByPubkey(activeValidatorWithIndex(1).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(0), index)
	require.Equal(t, map[[48]byte]uint64{
		activeValidatorWithIndex(1).PublicKey(): 0,
		activeValidatorWithIndex(2).PublicKey(): 1,
	}, source.publicKeyIndices.base.entries)

	for key, wantIndex := range map[uint64]uint64{3: 2, 4: 3} {
		index, ok = copied.ValidatorIndexByPubkey(activeValidatorWithIndex(key).PublicKey())
		require.True(t, ok)
		require.Equal(t, wantIndex, index)
	}
	index, ok = source.ValidatorIndexByPubkey(activeValidatorWithIndex(3).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(2), index)
	_, ok = source.ValidatorIndexByPubkey(activeValidatorWithIndex(4).PublicKey())
	require.False(t, ok)
	_, ok = sibling.ValidatorIndexByPubkey(activeValidatorWithIndex(4).PublicKey())
	require.False(t, ok)
	assertPubkeyIndexMatchesValidators(t, source)
	assertPubkeyIndexMatchesValidators(t, copied)
	assertPubkeyIndexMatchesValidators(t, sibling)

	unrelated := stateWithValidators(t, 100, 101)
	require.NoError(t, unrelated.CopyInto(copied))
	assertPubkeyIndexMatchesValidators(t, source)
	assertPubkeyIndexMatchesValidators(t, sibling)
	assertPubkeyIndexMatchesValidators(t, copied)
	for _, key := range []uint64{1, 3, 4} {
		_, ok = copied.ValidatorIndexByPubkey(activeValidatorWithIndex(key).PublicKey())
		require.False(t, ok)
	}
}

func TestDeferredPubkeyIndexFindsDeltaWithoutBuildingBase(t *testing.T) {
	beaconState := New(&clparams.MainnetBeaconConfig)
	beaconState.SetValidators(validatorSet(1, 2))
	duplicate := activeValidatorWithIndex(1)
	beaconState.AppendValidator(duplicate)
	appended := activeValidatorWithIndex(3)
	beaconState.AppendValidator(appended)

	index, ok := beaconState.ValidatorIndexByPubkey(duplicate.PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(2), index)
	require.Nil(t, beaconState.publicKeyIndices.base.entries)

	index, ok = beaconState.ValidatorIndexByPubkey(appended.PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(3), index)
	require.Nil(t, beaconState.publicKeyIndices.base.entries)
}

func TestDeferredPubkeyIndexShortReceiverDoesNotBuildBase(t *testing.T) {
	base := &pubkeyIndexBase{length: 2}
	short := New(&clparams.MainnetBeaconConfig)
	short.SetValidators(validatorSet(1))
	short.publicKeyIndices = pubkeyIndex{base: base}

	_, ok := short.ValidatorIndexByPubkey(activeValidatorWithIndex(1).PublicKey())
	require.False(t, ok)
	require.Nil(t, base.entries)

	full := New(&clparams.MainnetBeaconConfig)
	full.SetValidators(validatorSet(1, 2))
	full.publicKeyIndices = pubkeyIndex{base: base}
	index, ok := full.ValidatorIndexByPubkey(activeValidatorWithIndex(2).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(1), index)
	require.NotNil(t, base.entries)
	_, ok = short.ValidatorIndexByPubkey(activeValidatorWithIndex(1).PublicKey())
	require.False(t, ok)
}

func TestDeferredPubkeyIndexZeroLength(t *testing.T) {
	beaconState := New(&clparams.MainnetBeaconConfig)
	beaconState.SetValidators(validatorSet())
	require.Zero(t, beaconState.publicKeyIndices.base.length)
	require.Nil(t, beaconState.publicKeyIndices.base.entries)

	_, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(1).PublicKey())
	require.False(t, ok)
	require.NotNil(t, beaconState.publicKeyIndices.base.entries)
}

func TestDeferredPubkeyIndexDuplicateUsesLastIndex(t *testing.T) {
	beaconState := New(&clparams.MainnetBeaconConfig)
	validator := activeValidatorWithIndex(1)
	validators := validatorSet()
	validators.Append(validator)
	validators.Append(validator)
	beaconState.SetValidators(validators)

	index, ok := beaconState.ValidatorIndexByPubkey(validator.PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(1), index)
}

func TestPubkeyIndexCopyLineages(t *testing.T) {
	ancestor := stateWithValidators(t, 1, 2)
	require.NoError(t, ancestor.AddValidator(activeValidatorWithIndex(3), 32_000_000_000))

	left := New(ancestor.BeaconConfig())
	right := stateWithValidators(t, 100, 101, 102, 103)
	require.NoError(t, ancestor.CopyInto(left))
	require.NoError(t, ancestor.CopyInto(right))
	require.Same(t, ancestor.publicKeyIndices.base, left.publicKeyIndices.base)
	require.Same(t, ancestor.publicKeyIndices.base, right.publicKeyIndices.base)
	require.Len(t, left.publicKeyIndices.delta, 1)
	require.Len(t, right.publicKeyIndices.delta, 1)

	require.NoError(t, left.AddValidator(activeValidatorWithIndex(4), 32_000_000_000))
	require.NoError(t, right.AddValidator(activeValidatorWithIndex(5), 32_000_000_000))
	_, ancestorHasLeftKey := ancestor.ValidatorIndexByPubkey(activeValidatorWithIndex(4).PublicKey())
	require.False(t, ancestorHasLeftKey)
	_, rightHasLeftKey := right.ValidatorIndexByPubkey(activeValidatorWithIndex(4).PublicKey())
	require.False(t, rightHasLeftKey)
	assertPubkeyIndexMatchesValidators(t, ancestor)
	assertPubkeyIndexMatchesValidators(t, left)
	assertPubkeyIndexMatchesValidators(t, right)

	chained := New(ancestor.BeaconConfig())
	require.NoError(t, left.CopyInto(chained))
	require.Len(t, chained.publicKeyIndices.delta, 2)
	assertPubkeyIndexMatchesValidators(t, chained)
	_, foundRightOnly := chained.ValidatorIndexByPubkey(activeValidatorWithIndex(5).PublicKey())
	require.False(t, foundRightOnly)
}

func TestPubkeyIndexCopyIntoUnrelatedSpareWithLargeDelta(t *testing.T) {
	source := stateWithValidators(t, 1)
	for i := uint64(2); i < 4_098; i++ {
		require.NoError(t, source.AddValidator(activeValidatorWithIndex(i), 32_000_000_000))
	}
	target := stateWithValidators(t, 10_000, 10_001, 10_002)

	require.NoError(t, source.CopyInto(target))

	require.Same(t, source.publicKeyIndices.base, target.publicKeyIndices.base)
	assertPubkeyIndexMatchesValidators(t, target)
	_, oldLineageKeyFound := target.ValidatorIndexByPubkey(activeValidatorWithIndex(10_000).PublicKey())
	require.False(t, oldLineageKeyFound)
}

func TestPubkeyIndexCopyIntoBuildsDeltaFromCopiedValidators(t *testing.T) {
	source := stateWithValidators(t, 1, 2)
	require.NoError(t, source.AddValidator(activeValidatorWithIndex(3), 32_000_000_000))
	require.NoError(t, source.AddValidator(activeValidatorWithIndex(4), 32_000_000_000))
	source.publicKeyIndices.delta = map[[48]byte]uint64{
		activeValidatorWithIndex(3).PublicKey():  0,
		activeValidatorWithIndex(99).PublicKey(): 2,
	}
	target := stateWithValidators(t, 100)

	require.NoError(t, source.CopyInto(target))

	assertPubkeyIndexMatchesValidators(t, target)
	_, foundPoisonedKey := target.ValidatorIndexByPubkey(activeValidatorWithIndex(99).PublicKey())
	require.False(t, foundPoisonedKey)
}

func TestPubkeyIndexRegistryMutators(t *testing.T) {
	beaconState := stateWithValidators(t, 1, 2)
	require.NotNil(t, beaconState.publicKeyIndices.base.entries)

	appended := activeValidatorWithIndex(3)
	beaconState.AppendValidator(appended)
	assertPubkeyIndexMatchesValidators(t, beaconState)

	replacement := activeValidatorWithIndex(4)
	oldKey := beaconState.ValidatorSet().Get(0).PublicKey()
	beaconState.SetValidatorAtIndex(0, replacement)
	require.NotNil(t, beaconState.publicKeyIndices.base.entries)
	assertPubkeyIndexMatchesValidators(t, beaconState)
	_, ok := beaconState.ValidatorIndexByPubkey(oldKey)
	require.False(t, ok)

	validators := validatorSet(10, 11, 12)
	beaconState.SetValidatorSet(validators)
	require.NotNil(t, beaconState.publicKeyIndices.base.entries)
	assertPubkeyIndexMatchesValidators(t, beaconState)

	validators = validatorSet(20, 21)
	beaconState.SetValidators(validators)
	require.Nil(t, beaconState.publicKeyIndices.base.entries)
	assertPubkeyIndexMatchesValidators(t, beaconState)

	require.NoError(t, beaconState.InitBeaconState())
	require.NotNil(t, beaconState.publicKeyIndices.base.entries)
	assertPubkeyIndexMatchesValidators(t, beaconState)

	key := beaconState.ValidatorSet().Get(0).PublicKey()
	base := newPubkeyIndex(beaconState.ValidatorSet()).base
	base.entries[key] = math.MaxUint64
	beaconState.publicKeyIndices.base = base
	_, ok = beaconState.ValidatorIndexByPubkey(key)
	require.False(t, ok)

	missingKey := activeValidatorWithIndex(99).PublicKey()
	beaconState.publicKeyIndices.delta = map[[48]byte]uint64{missingKey: math.MaxUint64}
	_, ok = beaconState.ValidatorIndexByPubkey(missingKey)
	require.False(t, ok)
}

func TestPubkeyIndexDecodeSuccessAndFailure(t *testing.T) {
	source := stateWithValidators(t, 1, 2, 3)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)

	target := stateWithValidators(t, 100)
	require.NoError(t, target.DecodeSSZ(encoded, int(source.Version())))
	require.Nil(t, target.publicKeyIndices.base.entries)
	require.NoError(t, target.AddValidator(activeValidatorWithIndex(4), 32_000_000_000))

	index, ok := target.ValidatorIndexByPubkey(activeValidatorWithIndex(2).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(1), index)
	require.NotNil(t, target.publicKeyIndices.base.entries)
	_, ok = target.ValidatorIndexByPubkey(activeValidatorWithIndex(99).PublicKey())
	require.False(t, ok)
	index, ok = target.ValidatorIndexByPubkey(activeValidatorWithIndex(4).PublicKey())
	require.True(t, ok)
	require.Equal(t, uint64(3), index)

	require.Error(t, target.DecodeSSZ([]byte{1}, int(source.Version())))
	require.Nil(t, target.publicKeyIndices.base)
	_, ok = target.ValidatorIndexByPubkey(activeValidatorWithIndex(1).PublicKey())
	require.False(t, ok)
}

func TestPubkeyIndexCopyAfterFailedDecodeBuildsFreshBase(t *testing.T) {
	source := stateWithValidators(t, 1, 2, 3)
	require.Error(t, source.DecodeSSZ([]byte{1}, int(source.Version())))
	firstCopy := New(source.BeaconConfig())

	require.NoError(t, source.CopyInto(firstCopy))

	assertPubkeyIndexMatchesValidators(t, firstCopy)
	require.NotNil(t, firstCopy.publicKeyIndices.base)
	require.NotNil(t, firstCopy.publicKeyIndices.base.entries)
	require.Empty(t, firstCopy.publicKeyIndices.delta)

	secondCopy := New(source.BeaconConfig())
	require.NoError(t, firstCopy.CopyInto(secondCopy))
	assertPubkeyIndexMatchesValidators(t, secondCopy)
	require.NotNil(t, secondCopy.publicKeyIndices.base)
	require.Empty(t, secondCopy.publicKeyIndices.delta)
}

func TestPubkeyIndexSurvivesFuluToGloasUpgrade(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	beaconState := New(&cfg)
	beaconState.SetVersion(clparams.FuluVersion)
	for i := uint64(1); i <= cfg.SlotsPerEpoch; i++ {
		require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(i), 32_000_000_000))
	}
	require.NoError(t, beaconState.InitBeaconState())

	require.NoError(t, beaconState.UpgradeToGloas())

	assertPubkeyIndexMatchesValidators(t, beaconState)
}

func TestGloasBuilderOnboardingUsesBaseAndDeltaPubkeyIndex(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	beaconState := New(&cfg)
	beaconState.SetVersion(clparams.GloasVersion)
	require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(1), 32_000_000_000))
	require.NoError(t, beaconState.InitBeaconState())
	require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(2), 32_000_000_000))

	credentials := common.Hash{byte(cfg.BuilderWithdrawalPrefix)}
	pendingDeposits := solid.NewPendingDepositList(&cfg)
	for _, index := range []uint64{1, 2, 3} {
		pendingDeposits.Append(&solid.PendingDeposit{
			PubKey:                activeValidatorWithIndex(index).PublicKey(),
			WithdrawalCredentials: credentials,
			Amount:                1,
		})
	}
	beaconState.SetPendingDeposits(pendingDeposits)

	require.NoError(t, beaconState.onboardBuildersFromPendingDeposits())

	require.Equal(t, 2, beaconState.GetPendingDeposits().Len())
	require.Equal(t, common.Bytes48(activeValidatorWithIndex(1).PublicKey()), beaconState.GetPendingDeposits().Get(0).PubKey)
	require.Equal(t, common.Bytes48(activeValidatorWithIndex(2).PublicKey()), beaconState.GetPendingDeposits().Get(1).PubKey)
}

func TestPubkeyIndexRandomOperations(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	random := rand.New(rand.NewSource(1))
	universe := make(map[[48]byte]struct{})
	nextValidator := uint64(1)
	newValidator := func() solid.Validator {
		validator := activeValidatorWithIndex(nextValidator)
		universe[validator.PublicKey()] = struct{}{}
		nextValidator++
		return validator
	}
	newValidatorSet := func(count int) *solid.ValidatorSet {
		validators := solid.NewValidatorSet(int(cfg.ValidatorRegistryLimit))
		for range count {
			validators.Append(newValidator())
		}
		return validators
	}
	newState := func(count int) *CachingBeaconState {
		beaconState := New(&cfg)
		beaconState.SetVersion(clparams.AltairVersion)
		for range count {
			require.NoError(t, beaconState.AddValidator(newValidator(), 32_000_000_000))
		}
		require.NoError(t, beaconState.InitBeaconState())
		return beaconState
	}
	states := []*CachingBeaconState{newState(2), newState(2), newState(0)}

	for range 500 {
		target := random.Intn(len(states))
		switch random.Intn(10) {
		case 0:
			require.NoError(t, states[target].AddValidator(newValidator(), 32_000_000_000))
		case 1:
			states[target].AppendValidator(newValidator())
		case 2:
			source := random.Intn(len(states))
			if source == target {
				source = (source + 1) % len(states)
			}
			require.NoError(t, states[source].CopyInto(states[target]))
		case 3:
			states[target].SetValidators(newValidatorSet(2))
		case 4:
			states[target].SetValidatorSet(newValidatorSet(3))
		case 5:
			if states[target].ValidatorLength() > 0 {
				states[target].SetValidatorAtIndex(random.Intn(states[target].ValidatorLength()), newValidator())
			}
		case 6:
			require.NoError(t, states[target].InitBeaconState())
		case 7:
			source := (target + 1) % len(states)
			encoded, err := states[source].EncodeSSZ(nil)
			require.NoError(t, err)
			require.NoError(t, states[target].DecodeSSZ(encoded, int(states[source].Version())))
		case 8:
			require.Error(t, states[target].DecodeSSZ([]byte{1}, int(states[target].Version())))
			assertNoPubkeyIndexLookups(t, states[target], universe)
			source := (target + 1) % len(states)
			require.NoError(t, states[source].CopyInto(states[target]))
		case 9:
			source := (target + 1) % len(states)
			middle := (target + 2) % len(states)
			require.NoError(t, states[source].CopyInto(states[middle]))
			require.NoError(t, states[middle].CopyInto(states[target]))
		}
		for _, beaconState := range states {
			assertPubkeyIndexMatchesUniverse(t, beaconState, universe)
		}
	}
}

func TestPubkeyIndexSharedBaseConcurrentMutation(t *testing.T) {
	source := stateWithValidators(t, 1, 2, 3, 4)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	readers := make([]*CachingBeaconState, 8)
	for i := range readers {
		readers[i] = New(source.BeaconConfig())
		require.NoError(t, source.CopyInto(readers[i]))
	}

	var waitGroup sync.WaitGroup
	for _, reader := range readers {
		waitGroup.Go(func() {
			for range 1_000 {
				for index := range uint64(4) {
					got, ok := reader.ValidatorIndexByPubkey(activeValidatorWithIndex(index + 1).PublicKey())
					if !ok || got != index {
						t.Errorf("lookup %d returned (%d, %v)", index+1, got, ok)
						return
					}
				}
			}
		})
	}
	for offset := range uint64(90) {
		require.NoError(t, source.AddValidator(activeValidatorWithIndex(offset+10), 32_000_000_000))
	}
	require.NoError(t, source.InitBeaconState())
	require.NoError(t, source.DecodeSSZ(encoded, int(source.Version())))
	waitGroup.Wait()
}

func validatorWithIndex(index uint64) solid.Validator {
	validator := solid.NewValidator()
	var publicKey [48]byte
	binary.LittleEndian.PutUint64(publicKey[:8], index+1)
	validator.SetPublicKey(publicKey)
	return validator
}

func activeValidatorWithIndex(index uint64) solid.Validator {
	validator := validatorWithIndex(index)
	validator.SetEffectiveBalance(32_000_000_000)
	validator.SetActivationEpoch(0)
	validator.SetExitEpoch(math.MaxUint64)
	validator.SetWithdrawableEpoch(math.MaxUint64)
	return validator
}

func validatorSet(indices ...uint64) *solid.ValidatorSet {
	validators := solid.NewValidatorSet(int(clparams.MainnetBeaconConfig.ValidatorRegistryLimit))
	for _, index := range indices {
		validators.Append(activeValidatorWithIndex(index))
	}
	return validators
}

func stateWithValidators(t *testing.T, indices ...uint64) *CachingBeaconState {
	t.Helper()
	beaconState := New(&clparams.MainnetBeaconConfig)
	beaconState.SetVersion(clparams.AltairVersion)
	for _, index := range indices {
		require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(index), 32_000_000_000))
	}
	require.NoError(t, beaconState.InitBeaconState())
	return beaconState
}

func assertPubkeyIndexMatchesValidators(t *testing.T, beaconState *CachingBeaconState) {
	t.Helper()
	want := pubkeyIndexFromValidators(beaconState)
	for key, index := range want {
		got, ok := beaconState.ValidatorIndexByPubkey(key)
		require.True(t, ok)
		require.Equal(t, index, got)
	}
	_, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(math.MaxUint64).PublicKey())
	require.False(t, ok)
}

func assertPubkeyIndexMatchesUniverse(t *testing.T, beaconState *CachingBeaconState, universe map[[48]byte]struct{}) {
	t.Helper()
	want := pubkeyIndexFromValidators(beaconState)
	for key := range universe {
		got, ok := beaconState.ValidatorIndexByPubkey(key)
		wantIndex, wantOK := want[key]
		require.Equal(t, wantOK, ok)
		if wantOK {
			require.Equal(t, wantIndex, got)
		}
	}
}

func pubkeyIndexFromValidators(beaconState *CachingBeaconState) map[[48]byte]uint64 {
	want := make(map[[48]byte]uint64, beaconState.ValidatorLength())
	beaconState.ForEachValidator(func(validator solid.Validator, index, _ int) bool {
		want[validator.PublicKey()] = uint64(index)
		return true
	})
	return want
}

func assertNoPubkeyIndexLookups(t *testing.T, beaconState *CachingBeaconState, universe map[[48]byte]struct{}) {
	t.Helper()
	for key := range universe {
		_, ok := beaconState.ValidatorIndexByPubkey(key)
		require.False(t, ok)
	}
}
