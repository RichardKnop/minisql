package minisql

import (
	"encoding/binary"
	"fmt"
	"slices"
)

const invertedPostingCodecVersion byte = 1

type invertedPostingMode byte

const (
	invertedPostingModeRowIDs invertedPostingMode = iota + 1
	invertedPostingModePositions
)

type invertedPosting struct {
	RowID     RowID
	Positions []uint32
}

// encodeInvertedPostingList serializes postings into the v1 row-grouped codec.
// Row IDs are sorted and delta-encoded; positional postings also store sorted
// per-row position deltas.
func encodeInvertedPostingList(mode invertedPostingMode, postings []invertedPosting) ([]byte, error) {
	if len(postings) == 0 {
		return []byte{invertedPostingCodecVersion, byte(mode)}, nil
	}
	if mode != invertedPostingModeRowIDs && mode != invertedPostingModePositions {
		return nil, fmt.Errorf("unknown inverted posting mode %d", mode)
	}

	return encodeGroupedInvertedPostingList(mode, groupInvertedPostings(mode, postings))
}

// encodeGroupedInvertedPostingList serializes postings that are already sorted,
// grouped by row ID, and deduplicated. It is used by posting-tree block packing
// to avoid repeatedly regrouping prefixes while searching for a block boundary.
func encodeGroupedInvertedPostingList(mode invertedPostingMode, grouped []invertedPosting) ([]byte, error) {
	if mode != invertedPostingModeRowIDs && mode != invertedPostingModePositions {
		return nil, fmt.Errorf("unknown inverted posting mode %d", mode)
	}
	buf := make([]byte, 0, 2+len(grouped)*binary.MaxVarintLen64*2)
	buf = append(buf, invertedPostingCodecVersion, byte(mode))

	var (
		prevRowID RowID
		tmp       [binary.MaxVarintLen64]byte
	)
	for i, posting := range grouped {
		rowDelta := uint64(posting.RowID)
		if i > 0 {
			rowDelta = uint64(posting.RowID - prevRowID)
		}
		n := binary.PutUvarint(tmp[:], rowDelta)
		buf = append(buf, tmp[:n]...)

		if mode == invertedPostingModePositions {
			n = binary.PutUvarint(tmp[:], uint64(len(posting.Positions)))
			buf = append(buf, tmp[:n]...)
			var prevPosition uint32
			for j, position := range posting.Positions {
				positionDelta := uint64(position)
				if j > 0 {
					positionDelta = uint64(position - prevPosition)
				}
				n = binary.PutUvarint(tmp[:], positionDelta)
				buf = append(buf, tmp[:n]...)
				prevPosition = position
			}
		}
		prevRowID = posting.RowID
	}
	return buf, nil
}

// encodeTrailingInvertedPosting serializes one posting that follows prevRowID
// inside an existing block payload. The caller guarantees posting.RowID is newer.
func encodeTrailingInvertedPosting(mode invertedPostingMode, prevRowID RowID, posting invertedPosting) ([]byte, uint32) {
	var tmp [binary.MaxVarintLen64]byte
	buf := make([]byte, 0, binary.MaxVarintLen64*(2+len(posting.Positions)))

	n := binary.PutUvarint(tmp[:], uint64(posting.RowID-prevRowID))
	buf = append(buf, tmp[:n]...)
	if mode == invertedPostingModeRowIDs {
		return buf, 1
	}

	positions := append([]uint32(nil), posting.Positions...)
	slices.Sort(positions)
	positions = slices.Compact(positions)
	n = binary.PutUvarint(tmp[:], uint64(len(positions)))
	buf = append(buf, tmp[:n]...)
	var prevPosition uint32
	for i, position := range positions {
		positionDelta := uint64(position)
		if i > 0 {
			positionDelta = uint64(position - prevPosition)
		}
		n = binary.PutUvarint(tmp[:], positionDelta)
		buf = append(buf, tmp[:n]...)
		prevPosition = position
	}
	return buf, uint32(len(positions))
}

// decodeInvertedPostingList decodes the v1 row-grouped posting codec and
// returns the encoded mode together with sorted postings.
func decodeInvertedPostingList(encoded []byte) (invertedPostingMode, []invertedPosting, error) {
	if len(encoded) < 2 {
		return 0, nil, fmt.Errorf("decode inverted postings: short buffer")
	}
	if encoded[0] != invertedPostingCodecVersion {
		return 0, nil, fmt.Errorf("decode inverted postings: unsupported codec version %d", encoded[0])
	}
	mode := invertedPostingMode(encoded[1])
	if mode != invertedPostingModeRowIDs && mode != invertedPostingModePositions {
		return 0, nil, fmt.Errorf("decode inverted postings: unknown mode %d", mode)
	}

	postings := make([]invertedPosting, 0)
	var (
		prevRowID RowID
		offset    = 2
	)
	for offset < len(encoded) {
		rowDelta, n := binary.Uvarint(encoded[offset:])
		if n <= 0 {
			return 0, nil, fmt.Errorf("decode inverted posting row delta at byte %d", offset)
		}
		offset += n

		rowID := RowID(rowDelta)
		if len(postings) > 0 {
			rowID = prevRowID + RowID(rowDelta)
		}
		posting := invertedPosting{RowID: rowID}

		if mode == invertedPostingModePositions {
			positionCount, n := binary.Uvarint(encoded[offset:])
			if n <= 0 {
				return 0, nil, fmt.Errorf("decode inverted posting position count at byte %d", offset)
			}
			offset += n
			posting.Positions = make([]uint32, 0, positionCount)
			var prevPosition uint32
			for range positionCount {
				positionDelta, n := binary.Uvarint(encoded[offset:])
				if n <= 0 {
					return 0, nil, fmt.Errorf("decode inverted posting position delta at byte %d", offset)
				}
				offset += n
				position := uint32(positionDelta)
				if len(posting.Positions) > 0 {
					position = prevPosition + uint32(positionDelta)
				}
				posting.Positions = append(posting.Positions, position)
				prevPosition = position
			}
		}

		postings = append(postings, posting)
		prevRowID = rowID
	}
	return mode, postings, nil
}

// forEachInvertedPostingRowID decodes only row IDs from an encoded posting
// block. Positional payloads are skipped without allocating per-row positions,
// which keeps common single-term full-text scans lightweight.
func forEachInvertedPostingRowID(encoded []byte, fn func(RowID) error) (invertedPostingMode, error) {
	if len(encoded) < 2 {
		return 0, fmt.Errorf("decode inverted postings: short buffer")
	}
	if encoded[0] != invertedPostingCodecVersion {
		return 0, fmt.Errorf("decode inverted postings: unsupported codec version %d", encoded[0])
	}
	mode := invertedPostingMode(encoded[1])
	if mode != invertedPostingModeRowIDs && mode != invertedPostingModePositions {
		return 0, fmt.Errorf("decode inverted postings: unknown mode %d", mode)
	}

	var (
		prevRowID RowID
		offset    = 2
		haveRow   bool
	)
	for offset < len(encoded) {
		rowDelta, n := binary.Uvarint(encoded[offset:])
		if n <= 0 {
			return 0, fmt.Errorf("decode inverted posting row delta at byte %d", offset)
		}
		offset += n

		rowID := RowID(rowDelta)
		if haveRow {
			rowID = prevRowID + RowID(rowDelta)
		}
		if err := fn(rowID); err != nil {
			return 0, err
		}

		if mode == invertedPostingModePositions {
			positionCount, n := binary.Uvarint(encoded[offset:])
			if n <= 0 {
				return 0, fmt.Errorf("decode inverted posting position count at byte %d", offset)
			}
			offset += n
			for range positionCount {
				_, n := binary.Uvarint(encoded[offset:])
				if n <= 0 {
					return 0, fmt.Errorf("decode inverted posting position delta at byte %d", offset)
				}
				offset += n
			}
		}

		prevRowID = rowID
		haveRow = true
	}
	return mode, nil
}

func forEachInvertedPostingPosition(
	encoded []byte,
	fn func(RowID, []uint32) error,
) (invertedPostingMode, error) {
	if len(encoded) < 2 {
		return 0, fmt.Errorf("decode inverted postings: short buffer")
	}
	if encoded[0] != invertedPostingCodecVersion {
		return 0, fmt.Errorf("decode inverted postings: unsupported codec version %d", encoded[0])
	}
	mode := invertedPostingMode(encoded[1])
	if mode != invertedPostingModeRowIDs && mode != invertedPostingModePositions {
		return 0, fmt.Errorf("decode inverted postings: unknown mode %d", mode)
	}

	var (
		prevRowID RowID
		offset    = 2
		haveRow   bool
	)
	for offset < len(encoded) {
		rowDelta, n := binary.Uvarint(encoded[offset:])
		if n <= 0 {
			return 0, fmt.Errorf("decode inverted posting row delta at byte %d", offset)
		}
		offset += n

		rowID := RowID(rowDelta)
		if haveRow {
			rowID = prevRowID + RowID(rowDelta)
		}

		var positions []uint32
		if mode == invertedPostingModePositions {
			positionCount, n := binary.Uvarint(encoded[offset:])
			if n <= 0 {
				return 0, fmt.Errorf("decode inverted posting position count at byte %d", offset)
			}
			offset += n
			positions = make([]uint32, 0, positionCount)
			var prevPosition uint32
			for range positionCount {
				positionDelta, n := binary.Uvarint(encoded[offset:])
				if n <= 0 {
					return 0, fmt.Errorf("decode inverted posting position delta at byte %d", offset)
				}
				offset += n
				position := uint32(positionDelta)
				if len(positions) > 0 {
					position = prevPosition + uint32(positionDelta)
				}
				positions = append(positions, position)
				prevPosition = position
			}
		}

		if err := fn(rowID, positions); err != nil {
			return 0, err
		}
		prevRowID = rowID
		haveRow = true
	}
	return mode, nil
}

func forEachInvertedPostingDocCount(encoded []byte, fn func(RowID, uint32) error) (invertedPostingMode, error) {
	if len(encoded) < 2 {
		return 0, fmt.Errorf("decode inverted postings: short buffer")
	}
	if encoded[0] != invertedPostingCodecVersion {
		return 0, fmt.Errorf("decode inverted postings: unsupported codec version %d", encoded[0])
	}
	mode := invertedPostingMode(encoded[1])
	if mode != invertedPostingModeRowIDs && mode != invertedPostingModePositions {
		return 0, fmt.Errorf("decode inverted postings: unknown mode %d", mode)
	}

	var (
		prevRowID RowID
		offset    = 2
		haveRow   bool
	)
	for offset < len(encoded) {
		rowDelta, n := binary.Uvarint(encoded[offset:])
		if n <= 0 {
			return 0, fmt.Errorf("decode inverted posting row delta at byte %d", offset)
		}
		offset += n

		rowID := RowID(rowDelta)
		if haveRow {
			rowID = prevRowID + RowID(rowDelta)
		}

		var positionCount uint32 = 1
		if mode == invertedPostingModePositions {
			count, n := binary.Uvarint(encoded[offset:])
			if n <= 0 {
				return 0, fmt.Errorf("decode inverted posting position count at byte %d", offset)
			}
			offset += n
			positionCount = uint32(count)
			for range count {
				_, n := binary.Uvarint(encoded[offset:])
				if n <= 0 {
					return 0, fmt.Errorf("decode inverted posting position delta at byte %d", offset)
				}
				offset += n
			}
		}

		if err := fn(rowID, positionCount); err != nil {
			return 0, err
		}
		prevRowID = rowID
		haveRow = true
	}
	return mode, nil
}

// groupInvertedPostings canonicalizes postings before encoding. JSON-style
// row-only postings deduplicate row IDs; full-text postings merge and sort
// positions by row ID.
func groupInvertedPostings(mode invertedPostingMode, postings []invertedPosting) []invertedPosting {
	sorted := append([]invertedPosting(nil), postings...)
	slices.SortFunc(sorted, compareInvertedPostings)

	grouped := make([]invertedPosting, 0, len(sorted))
	for _, posting := range sorted {
		if len(grouped) == 0 || grouped[len(grouped)-1].RowID != posting.RowID {
			next := invertedPosting{RowID: posting.RowID}
			if mode == invertedPostingModePositions {
				next.Positions = append(next.Positions, posting.Positions...)
			}
			grouped = append(grouped, next)
			continue
		}
		if mode == invertedPostingModePositions {
			grouped[len(grouped)-1].Positions = append(grouped[len(grouped)-1].Positions, posting.Positions...)
		}
	}

	if mode == invertedPostingModePositions {
		for i := range grouped {
			slices.Sort(grouped[i].Positions)
			grouped[i].Positions = slices.Compact(grouped[i].Positions)
		}
	}
	return grouped
}

// groupInvertedPostingsInPlace sorts and groups postings in-place, returning a
// subslice of the input. The caller must not retain the input slice after this
// call. Unlike groupInvertedPostings it avoids the up-front copy, so the common
// case (all unique RowIDs) has zero allocations. Used on the hot insert/update
// path where the caller always owns the slice.
func groupInvertedPostingsInPlace(mode invertedPostingMode, postings []invertedPosting) []invertedPosting {
	if len(postings) == 0 {
		return postings
	}
	if len(postings) == 1 {
		if mode == invertedPostingModePositions {
			slices.Sort(postings[0].Positions)
			postings[0].Positions = slices.Compact(postings[0].Positions)
		}
		return postings
	}
	slices.SortFunc(postings, compareInvertedPostings)
	out := 0
	for i := 1; i < len(postings); i++ {
		if postings[out].RowID == postings[i].RowID {
			if mode == invertedPostingModePositions {
				postings[out].Positions = append(postings[out].Positions, postings[i].Positions...)
			}
		} else {
			out += 1
			postings[out] = postings[i]
		}
	}
	postings = postings[:out+1]
	if mode == invertedPostingModePositions {
		for i := range postings {
			slices.Sort(postings[i].Positions)
			postings[i].Positions = slices.Compact(postings[i].Positions)
		}
	}
	return postings
}

// compareInvertedPostings orders postings by row ID, then by first position for
// positional postings. Canonical ordering keeps encoded payloads deterministic.
func compareInvertedPostings(a, b invertedPosting) int {
	if a.RowID < b.RowID {
		return -1
	}
	if a.RowID > b.RowID {
		return 1
	}
	if len(a.Positions) == 0 || len(b.Positions) == 0 {
		return 0
	}
	if a.Positions[0] < b.Positions[0] {
		return -1
	}
	if a.Positions[0] > b.Positions[0] {
		return 1
	}
	return 0
}
