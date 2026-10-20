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
	"sync"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// All states sharing a base have identical pubkeys in [0, length), so the first lookup may build it from any receiver.
type pubkeyIndexBase struct {
	entries map[[48]byte]uint64
	length  int
	once    sync.Once
}

func (b *pubkeyIndexBase) get(key [48]byte, validators *solid.ValidatorSet) (uint64, bool) {
	if b == nil || validators.Length() < b.length {
		return 0, false
	}
	b.build(validators)
	index, ok := b.entries[key]
	return index, ok
}

func (b *pubkeyIndexBase) build(validators *solid.ValidatorSet) {
	b.once.Do(func() {
		entries := make(map[[48]byte]uint64, b.length)
		for index := range b.length {
			entries[validators.Get(index).PublicKey()] = uint64(index)
		}
		b.entries = entries
	})
}

type pubkeyIndex struct {
	base  *pubkeyIndexBase
	delta map[[48]byte]uint64
}

func newPubkeyIndex(validators *solid.ValidatorSet) pubkeyIndex {
	base := &pubkeyIndexBase{length: validators.Length()}
	base.build(validators)
	return pubkeyIndex{base: base}
}

func newDeferredPubkeyIndex(length int) pubkeyIndex {
	return pubkeyIndex{base: &pubkeyIndexBase{length: length}}
}

func pubkeyIndexFromBase(base *pubkeyIndexBase, validators *solid.ValidatorSet) pubkeyIndex {
	if base == nil {
		return newPubkeyIndex(validators)
	}
	deltaLength := max(validators.Length()-base.length, 0)
	copied := pubkeyIndex{base: base}
	if deltaLength > 0 {
		copied.delta = make(map[[48]byte]uint64, deltaLength)
	}
	for offset := range deltaLength {
		index := base.length + offset
		copied.delta[validators.Get(index).PublicKey()] = uint64(index)
	}
	return copied
}

func (p *pubkeyIndex) add(key [48]byte, index uint64) {
	if p.delta == nil {
		p.delta = make(map[[48]byte]uint64)
	}
	p.delta[key] = index
}

func (p *pubkeyIndex) get(key [48]byte, validators *solid.ValidatorSet) (uint64, bool) {
	validatorCount := validators.Length()
	if index, ok := p.delta[key]; ok && index < uint64(validatorCount) {
		return index, true
	}
	index, ok := p.base.get(key, validators)
	return index, ok && index < uint64(validatorCount)
}
