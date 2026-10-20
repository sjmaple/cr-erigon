package solid

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
)

func TestValidatorSetProgressiveRootMatchesReference(t *testing.T) {
	for _, count := range []int{0, 1, 5, 21, 85, 341} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			validators := NewValidatorSet(1_000_000)
			for i := range count {
				validator := make(Validator, validatorSize)
				validator[0] = byte(i + 1)
				validators.Append(validator)
			}

			want := validatorSetProgressiveRootReference(t, validators)
			got, err := validators.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)

			if count > 0 {
				validator := make(Validator, validatorSize)
				validator[0] = 0xff
				validators.Set(count/2, validator)
				want = validatorSetProgressiveRootReference(t, validators)
				got, err = validators.HashSSZProgressive()
				require.NoError(t, err)
				require.Equal(t, want, got)
			}

			appended := make(Validator, validatorSize)
			appended[0] = 0xee
			validators.Append(appended)
			want = validatorSetProgressiveRootReference(t, validators)
			got, err = validators.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)

			copied := NewValidatorSet(validators.c)
			validators.CopyTo(copied)
			got, err = copied.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestValidatorSetSwitchesMerkleCacheMode(t *testing.T) {
	validators := NewValidatorSet(16)
	validators.Append(make(Validator, validatorSize))
	_, err := validators.HashSSZ()
	require.NoError(t, err)
	require.NotNil(t, validators.MerkleTree)

	validators.SetProgressiveHashing(true)
	require.Nil(t, validators.MerkleTree)
	_, err = validators.HashSSZProgressive()
	require.NoError(t, err)
	require.NotNil(t, validators.progressiveTrees)

	validators.SetProgressiveHashing(false)
	require.Nil(t, validators.progressiveTrees)
	_, err = validators.HashSSZ()
	require.NoError(t, err)
	require.NotNil(t, validators.MerkleTree)
}

func TestValidatorSetCopyToReusesProgressiveTrees(t *testing.T) {
	for _, count := range []int{1, 5, 21, 85, 341, 10_000} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			source := validatorSetWithCount(count)
			_, err := source.HashSSZProgressive()
			require.NoError(t, err)
			target := validatorSetWithCount(count + 1)
			_, err = target.HashSSZProgressive()
			require.NoError(t, err)
			oldTrees := append([]*merkle_tree.MerkleTree(nil), target.progressiveTrees...)

			source.CopyTo(target)

			for i := range min(len(oldTrees), len(target.progressiveTrees)) {
				require.Same(t, oldTrees[i], target.progressiveTrees[i])
			}
			require.Equal(t, validatorSetProgressiveRootReference(t, source), mustProgressiveRoot(t, target))

			sourceRoot := mustProgressiveRoot(t, source)
			target.Set(count/2, validatorForProgressiveTest(0xee))
			require.Equal(t, sourceRoot, mustProgressiveRoot(t, source))
			require.Equal(t, validatorSetProgressiveRootReference(t, target), mustProgressiveRoot(t, target))

			targetRoot := mustProgressiveRoot(t, target)
			source.Set(count/2, validatorForProgressiveTest(0xdd))
			require.Equal(t, targetRoot, mustProgressiveRoot(t, target))
			require.Equal(t, validatorSetProgressiveRootReference(t, source), mustProgressiveRoot(t, source))
		})
	}
}

func TestValidatorSetCopyToProgressiveTreeShapes(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		sourceCount int
		targetCount int
	}{
		{name: "target fewer segments", sourceCount: 341, targetCount: 1},
		{name: "target more segments", sourceCount: 5, targetCount: 341},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			source := validatorSetWithCount(testCase.sourceCount)
			target := validatorSetWithCount(testCase.targetCount)
			_, err := source.HashSSZProgressive()
			require.NoError(t, err)
			_, err = target.HashSSZProgressive()
			require.NoError(t, err)

			source.CopyTo(target)

			require.Len(t, target.progressiveTrees, len(source.progressiveTrees))
			require.Equal(t, validatorSetProgressiveRootReference(t, source), mustProgressiveRoot(t, target))
		})
	}
}

func TestValidatorSetCopyToTargetAppendsAcrossProgressiveSegments(t *testing.T) {
	for _, testCase := range []struct {
		start int
		end   int
	}{
		{start: 20, end: 22},
		{start: 84, end: 86},
		{start: 340, end: 342},
	} {
		t.Run(strconv.Itoa(testCase.start), func(t *testing.T) {
			source := validatorSetWithCount(testCase.start)
			_, err := source.HashSSZProgressive()
			require.NoError(t, err)
			target := validatorSetWithCount(1)
			_, err = target.HashSSZProgressive()
			require.NoError(t, err)

			source.CopyTo(target)
			sourceRoot := mustProgressiveRoot(t, source)
			for offset := range testCase.end - testCase.start {
				index := testCase.start + offset
				target.Append(validatorForProgressiveTest(byte(index + 1)))
			}

			require.Equal(t, validatorSetProgressiveRootReference(t, target), mustProgressiveRoot(t, target))
			require.Equal(t, sourceRoot, mustProgressiveRoot(t, source))
		})
	}
}

func TestValidatorSetCopyToHandlesNilProgressiveTree(t *testing.T) {
	source := validatorSetWithCount(5)
	_, err := source.HashSSZProgressive()
	require.NoError(t, err)
	source.progressiveTrees[1] = nil
	target := validatorSetWithCount(5)
	_, err = target.HashSSZProgressive()
	require.NoError(t, err)

	require.NotPanics(t, func() { source.CopyTo(target) })
	require.Nil(t, target.progressiveTrees[1])
}

func TestValidatorSetCopyToSwitchesHashingModes(t *testing.T) {
	fulu := validatorSetWithCount(5)
	fuluReference := validatorSetWithCount(5)
	wantFuluRoot, err := fuluReference.HashSSZ()
	require.NoError(t, err)
	_, err = fulu.HashSSZ()
	require.NoError(t, err)
	gloas := validatorSetWithCount(5)
	_, err = gloas.HashSSZProgressive()
	require.NoError(t, err)

	fulu.CopyTo(gloas)
	require.Nil(t, gloas.progressiveTrees)
	require.NotNil(t, gloas.MerkleTree)
	gotFuluRoot, err := gloas.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantFuluRoot, gotFuluRoot)

	gloas.SetProgressiveHashing(true)
	_, err = gloas.HashSSZProgressive()
	require.NoError(t, err)
	gloas.CopyTo(fulu)
	require.Nil(t, fulu.MerkleTree)
	require.NotNil(t, fulu.progressiveTrees)
	require.Equal(t, validatorSetProgressiveRootReference(t, gloas), mustProgressiveRoot(t, fulu))
}

func TestValidatorSetCopyToRejectsSelfCopy(t *testing.T) {
	validators := validatorSetWithCount(5)
	_, err := validators.HashSSZProgressive()
	require.NoError(t, err)
	require.PanicsWithValue(t, "ValidatorSet.CopyTo: source and target must differ", func() {
		validators.CopyTo(validators)
	})
}

func validatorSetWithCount(count int) *ValidatorSet {
	validators := NewValidatorSet(1_000_000)
	for i := range count {
		validators.Append(validatorForProgressiveTest(byte(i + 1)))
	}
	return validators
}

func validatorForProgressiveTest(firstByte byte) Validator {
	validator := make(Validator, validatorSize)
	validator[0] = firstByte
	return validator
}

func mustProgressiveRoot(t *testing.T, validators *ValidatorSet) [32]byte {
	t.Helper()
	root, err := validators.HashSSZProgressive()
	require.NoError(t, err)
	return root
}

func validatorSetProgressiveRootReference(t *testing.T, validators *ValidatorSet) [32]byte {
	roots := make([][32]byte, validators.l)
	hashBuffer := make([]byte, 8*32)
	for i := range roots {
		require.NoError(t, validators.Get(i).CopyHashBufferTo(hashBuffer))
		require.NoError(t, merkle_tree.MerkleRootFromFlatLeaves(hashBuffer, roots[i][:]))
	}
	root, err := merkle_tree.ProgressiveListRoot(roots, uint64(validators.l))
	require.NoError(t, err)
	return root
}
