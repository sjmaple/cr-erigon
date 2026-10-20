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
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

const (
	benchmarkValidatorCount = 1_140_000
	benchmarkValidatorDelta = 1_000
)

func BenchmarkBeaconStateCopyInto(b *testing.B) {
	for _, version := range []struct {
		name    string
		version clparams.StateVersion
	}{
		{name: "fulu", version: clparams.FuluVersion},
		{name: "gloas", version: clparams.GloasVersion},
	} {
		b.Run(version.name, func(b *testing.B) {
			source := benchmarkBeaconState(b, version.version)
			target := New(source.BeaconConfig())
			target.SetVersion(version.version)
			require.NoError(b, source.CopyInto(target))

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				require.NoError(b, source.CopyInto(target))
			}
		})
	}
}

func BenchmarkBeaconStateDecodeSSZ(b *testing.B) {
	source := benchmarkBeaconStateWithDelta(b, clparams.Phase0Version, 0)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(b, err)
	key := activeValidatorWithIndex(benchmarkValidatorCount - 1).PublicKey()

	for _, withLookup := range []bool{false, true} {
		name := "decode"
		if withLookup {
			name = "decode_and_lookup"
		}
		b.Run(name, func(b *testing.B) {
			target := New(source.BeaconConfig())
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				require.NoError(b, target.DecodeSSZ(encoded, int(source.Version())))
				if withLookup {
					index, ok := target.ValidatorIndexByPubkey(key)
					require.True(b, ok)
					require.Equal(b, uint64(benchmarkValidatorCount-1), index)
				}
			}
		})
	}
}

func BenchmarkValidatorIndexLookup(b *testing.B) {
	state := benchmarkBeaconState(b, clparams.FuluVersion)
	keys := [][48]byte{
		activeValidatorWithIndex(0).PublicKey(),
		activeValidatorWithIndex(benchmarkValidatorCount / 2).PublicKey(),
		activeValidatorWithIndex(benchmarkValidatorCount + benchmarkValidatorDelta - 1).PublicKey(),
	}

	b.Run("current", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_, _ = state.ValidatorIndexByPubkey(keys[i%len(keys)])
		}
	})
}

func benchmarkBeaconState(b *testing.B, version clparams.StateVersion) *CachingBeaconState {
	return benchmarkBeaconStateWithDelta(b, version, benchmarkValidatorDelta)
}

func benchmarkBeaconStateWithDelta(b *testing.B, version clparams.StateVersion, validatorDelta int) *CachingBeaconState {
	b.Helper()
	state := New(&clparams.MainnetBeaconConfig)
	state.SetVersion(version)
	validators := solid.NewValidatorSet(int(state.BeaconConfig().ValidatorRegistryLimit))
	balances := make([]uint64, benchmarkValidatorCount)
	inactivityScores := make([]uint64, benchmarkValidatorCount)
	for i := range benchmarkValidatorCount {
		validators.Append(activeValidatorWithIndex(uint64(i)))
		balances[i] = 32_000_000_000
		inactivityScores[i] = uint64(i % 4)
	}
	state.SetValidators(validators)
	state.SetBalances(solid.NewUint64ListSSZFromSlice(int(state.BeaconConfig().ValidatorRegistryLimit), balances))
	state.SetInactivityScores(inactivityScores)
	previousParticipation := solid.NewParticipationBitList(benchmarkValidatorCount, int(state.BeaconConfig().ValidatorRegistryLimit))
	currentParticipation := solid.NewParticipationBitList(benchmarkValidatorCount, int(state.BeaconConfig().ValidatorRegistryLimit))
	previousParticipation.Set(0, 1)
	currentParticipation.Set(benchmarkValidatorCount-1, 2)
	state.SetPreviousEpochParticipation(previousParticipation)
	state.SetCurrentEpochParticipation(currentParticipation)
	require.NoError(b, state.InitBeaconState())
	for i := range validatorDelta {
		require.NoError(b, state.AddValidator(activeValidatorWithIndex(uint64(benchmarkValidatorCount+i)), 32_000_000_000))
	}
	_, err := state.HashSSZ()
	require.NoError(b, err)
	return state
}
