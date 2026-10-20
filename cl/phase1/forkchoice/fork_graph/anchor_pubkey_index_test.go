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

package fork_graph

import (
	"encoding/binary"
	"math"
	"runtime"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func TestNewForkGraphDiskWarmsAnchorPubkeyIndex(t *testing.T) {
	cold, key := decodedAnchorWithValidators(t, 2_048)
	coldBytes := pubkeyLookupAllocatedBytes(t, cold, key)
	require.Greater(t, coldBytes, uint64(64<<10))

	anchor, key := decodedAnchorWithValidators(t, 2_048)
	_, err := NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)

	require.Less(t, pubkeyLookupAllocatedBytes(t, anchor, key), coldBytes/4)
}

func decodedAnchorWithValidators(t *testing.T, count int) (*state.CachingBeaconState, [48]byte) {
	t.Helper()
	source := state.New(&clparams.MainnetBeaconConfig)
	var key [48]byte
	for index := range count {
		validator := solid.NewValidator()
		binary.LittleEndian.PutUint64(key[:8], uint64(index+1))
		validator.SetPublicKey(key)
		validator.SetEffectiveBalance(32_000_000_000)
		validator.SetActivationEpoch(0)
		validator.SetExitEpoch(math.MaxUint64)
		validator.SetWithdrawableEpoch(math.MaxUint64)
		require.NoError(t, source.AddValidator(validator, 32_000_000_000))
	}
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	decoded := state.New(source.BeaconConfig())
	require.NoError(t, decoded.DecodeSSZ(encoded, int(source.Version())))
	return decoded, key
}

func pubkeyLookupAllocatedBytes(t *testing.T, beaconState *state.CachingBeaconState, key [48]byte) uint64 {
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
