package minisql

import (
	"encoding/binary"
	"fmt"
	"slices"
)

const (
	fullTextPostingPositionBits = 32
	maxFullTextPostingComponent = uint64(^uint32(0))
)

type fullTextPosting struct {
	RowID    RowID
	Position uint32
}

// encodeFullTextPosting packs one positional posting into a RowID-sized value.
// The current v1 full-text index stores this packed value in the existing
// non-unique B+ tree posting slots.
func encodeFullTextPosting(posting fullTextPosting) (RowID, error) {
	if uint64(posting.RowID) > maxFullTextPostingComponent {
		return 0, fmt.Errorf("full-text row id %d exceeds positional posting limit", posting.RowID)
	}
	return RowID(uint64(posting.RowID)<<fullTextPostingPositionBits | uint64(posting.Position)), nil
}

// decodeFullTextPosting unpacks the RowID-sized value stored by the v1 full-text
// index back into its logical row ID and token position.
func decodeFullTextPosting(posting RowID) fullTextPosting {
	return fullTextPosting{
		RowID:    RowID(uint64(posting) >> fullTextPostingPositionBits),
		Position: uint32(posting),
	}
}

// encodeFullTextPostingList serializes postings as sorted row/position deltas
// encoded with unsigned varints. This is preparation for a future dedicated
// inverted-index payload format; the current on-disk index still uses
// encodeFullTextPosting for each individual posting.
func encodeFullTextPostingList(postings []fullTextPosting) ([]byte, error) {
	if len(postings) == 0 {
		return nil, nil
	}

	sorted := append([]fullTextPosting(nil), postings...)
	slices.SortFunc(sorted, compareFullTextPostings)

	// PutUvarint writes into tmp below; this output buffer is only the append
	// destination. Pre-size for the worst case so encoding does not repeatedly grow it.
	buf := make([]byte, 0, len(sorted)*2*binary.MaxVarintLen64)
	var (
		prevRowID    RowID
		prevPosition uint32
		tmp          [binary.MaxVarintLen64]byte
	)
	for i, posting := range sorted {
		if uint64(posting.RowID) > maxFullTextPostingComponent {
			return nil, fmt.Errorf("full-text row id %d exceeds positional posting limit", posting.RowID)
		}

		var (
			rowDelta      = uint64(posting.RowID)
			positionDelta = uint64(posting.Position)
		)
		if i > 0 {
			rowDelta = uint64(posting.RowID - prevRowID)
			if posting.RowID == prevRowID {
				positionDelta = uint64(posting.Position - prevPosition)
			}
		}

		n := binary.PutUvarint(tmp[:], rowDelta)
		buf = append(buf, tmp[:n]...)
		n = binary.PutUvarint(tmp[:], positionDelta)
		buf = append(buf, tmp[:n]...)

		prevRowID = posting.RowID
		prevPosition = posting.Position
	}
	return buf, nil
}

// decodeFullTextPostingList reverses encodeFullTextPostingList and reconstructs
// the sorted positional postings from row/position deltas.
func decodeFullTextPostingList(encoded []byte) ([]fullTextPosting, error) {
	if len(encoded) == 0 {
		return nil, nil
	}

	postings := make([]fullTextPosting, 0)
	var (
		prevRowID    RowID
		prevPosition uint32
		offset       int
	)
	for offset < len(encoded) {
		rowDelta, n := binary.Uvarint(encoded[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("decode full-text posting row delta at byte %d", offset)
		}
		offset += n

		positionDelta, n := binary.Uvarint(encoded[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("decode full-text posting position delta at byte %d", offset)
		}
		offset += n

		rowID := RowID(rowDelta)
		position := uint32(positionDelta)
		if len(postings) > 0 {
			rowID = prevRowID + RowID(rowDelta)
			if rowID == prevRowID {
				position = prevPosition + uint32(positionDelta)
			}
		}

		posting := fullTextPosting{RowID: rowID, Position: position}
		postings = append(postings, posting)
		prevRowID = posting.RowID
		prevPosition = posting.Position
	}
	return postings, nil
}

// compareFullTextPostings orders postings by row ID first and token position
// second. Keeping a single canonical order makes delta encoding deterministic.
func compareFullTextPostings(a, b fullTextPosting) int {
	if a.RowID < b.RowID {
		return -1
	}
	if a.RowID > b.RowID {
		return 1
	}
	if a.Position < b.Position {
		return -1
	}
	if a.Position > b.Position {
		return 1
	}
	return 0
}
