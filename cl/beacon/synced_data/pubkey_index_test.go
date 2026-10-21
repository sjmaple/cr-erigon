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

package synced_data

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestHeadStatePublishWarmsPubkeyIndex(t *testing.T) {
	cold, key := decodedHeadStateWithValidators(t, 2_048)
	coldBytes := statePubkeyLookupAllocatedBytes(t, cold, key)
	require.Greater(t, coldBytes, uint64(64<<10))

	t.Run("OnHeadStateFrom", func(t *testing.T) {
		manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
		head, key := decodedHeadStateWithValidators(t, 2_048)
		published, err := manager.OnHeadStateFrom(common.Hash{1}, func(common.Hash, *state.CachingBeaconState) (*state.CachingBeaconState, error) {
			return head, nil
		})
		require.NoError(t, err)
		require.True(t, published)
		require.Less(t, managerPubkeyLookupAllocatedBytes(t, manager, key), coldBytes/4)
	})

	t.Run("OnHeadStateWithBlockRoot", func(t *testing.T) {
		manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
		head, key := decodedHeadStateWithValidators(t, 2_048)
		require.NoError(t, manager.OnHeadStateWithBlockRoot(head, common.Hash{1}))
		require.Less(t, managerPubkeyLookupAllocatedBytes(t, manager, key), coldBytes/4)
	})
}

func decodedHeadStateWithValidators(t *testing.T, count int) (*state.CachingBeaconState, common.Bytes48) {
	t.Helper()
	source := bigValidatorState(t, count)
	key, err := source.ValidatorPublicKey(count - 1)
	require.NoError(t, err)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	decoded := state.New(source.BeaconConfig())
	require.NoError(t, decoded.DecodeSSZ(encoded, int(source.Version())))
	return decoded, key
}

func statePubkeyLookupAllocatedBytes(t *testing.T, beaconState *state.CachingBeaconState, key common.Bytes48) uint64 {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	index, found := beaconState.ValidatorIndexByPubkey(key)
	runtime.ReadMemStats(&after)
	require.True(t, found)
	require.Equal(t, uint64(beaconState.ValidatorLength()-1), index)
	return after.TotalAlloc - before.TotalAlloc
}

func managerPubkeyLookupAllocatedBytes(t *testing.T, manager *SyncedDataManager, key common.Bytes48) uint64 {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	index, found, err := manager.ValidatorIndexByPublicKey(key)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(2_047), index)
	return after.TotalAlloc - before.TotalAlloc
}
