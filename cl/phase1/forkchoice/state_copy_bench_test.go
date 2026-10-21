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
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func BenchmarkCurrentStateCopy(b *testing.B) {
	current := state.New(&clparams.MainnetBeaconConfig)
	for i := range 100_000 {
		validator := solid.NewValidator()
		var publicKey [48]byte
		binary.BigEndian.PutUint64(publicKey[:], uint64(i))
		validator.SetPublicKey(publicKey)
		validator.SetActivationEpoch(0)
		validator.SetExitEpoch(^uint64(0))
		validator.SetEffectiveBalance(32_000_000_000)
		if err := current.AddValidator(validator, 32_000_000_000); err != nil {
			b.Fatal(err)
		}
	}
	root := common.Hash{0x01}
	store := &ForkChoiceStore{forkGraph: &currentStateForkGraph{root: root, current: current}}

	b.Run("new destination", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := store.GetStateAtBlockRoot(root); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("reused destination", func(b *testing.B) {
		spare := state.New(&clparams.MainnetBeaconConfig)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var err error
			spare, err = store.CopyStateAtBlockRootInto(root, spare)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
