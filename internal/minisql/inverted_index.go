package minisql

import (
	"context"
	"errors"
	"fmt"
)

var errInvertedIndexStorageNotImplemented = errors.New("dedicated inverted index storage mutation is not implemented yet")

type invertedIndexPostingMode = invertedPostingMode

const (
	invertedIndexPostingModeRowIDs    = invertedPostingModeRowIDs
	invertedIndexPostingModePositions = invertedPostingModePositions
)

type invertedPostingStats struct {
	DocFreq      uint32
	PostingCount uint32
}

type invertedPostingIterator interface {
	NextBlock(context.Context) (invertedPostingBlock, bool, error)
}

type invertedIndex interface {
	GetRootPageIdx() PageIndex
	Mode() invertedIndexPostingMode
	Insert(ctx context.Context, term string, posting invertedPosting) error
	Delete(ctx context.Context, term string, posting invertedPosting) error
	Lookup(ctx context.Context, term string) (invertedPostingIterator, error)
	Stats(ctx context.Context, term string) (invertedPostingStats, error)
}

type dedicatedInvertedIndex struct {
	pager       TxPager
	rootPageIdx PageIndex
	mode        invertedIndexPostingMode
	name        string
}

var _ invertedIndex = (*dedicatedInvertedIndex)(nil)

// NewDedicatedInvertedIndex opens a dedicated inverted-index skeleton backed by
// the new inverted entry/posting page format. This increment only owns root-page
// initialization; posting mutation lands in the next storage step.
func NewDedicatedInvertedIndex(name string, mode invertedIndexPostingMode, pager TxPager, rootPageIdx PageIndex) (*dedicatedInvertedIndex, error) {
	if mode != invertedIndexPostingModeRowIDs && mode != invertedIndexPostingModePositions {
		return nil, fmt.Errorf("unknown inverted index posting mode %d", mode)
	}
	return &dedicatedInvertedIndex{
		name:        name,
		mode:        mode,
		pager:       pager,
		rootPageIdx: rootPageIdx,
	}, nil
}

// GetRootPageIdx returns the root page of the inverted entry tree.
func (idx *dedicatedInvertedIndex) GetRootPageIdx() PageIndex {
	return idx.rootPageIdx
}

// Mode returns whether this index stores row-only or positional postings.
func (idx *dedicatedInvertedIndex) Mode() invertedIndexPostingMode {
	return idx.mode
}

// InitRootPage initializes the root page as an empty inverted entry leaf. It is
// idempotent for already-initialized inverted entry roots.
func (idx *dedicatedInvertedIndex) InitRootPage(ctx context.Context) error {
	page, err := idx.pager.ReadPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("read inverted index root: %w", err)
	}
	if page.InvertedEntryPage != nil && len(page.InvertedEntryPage.Cells) > 0 {
		return nil
	}

	page, err = idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("modify inverted index root: %w", err)
	}
	page.Clear()
	page.InvertedEntryPage = NewInvertedEntryPage(true)
	return nil
}

// Insert will add one term posting once inline posting-list storage is implemented.
func (idx *dedicatedInvertedIndex) Insert(context.Context, string, invertedPosting) error {
	return errInvertedIndexStorageNotImplemented
}

// Delete will remove one term posting once inline posting-list storage is implemented.
func (idx *dedicatedInvertedIndex) Delete(context.Context, string, invertedPosting) error {
	return errInvertedIndexStorageNotImplemented
}

// Lookup will return a posting iterator once inline posting-list storage is implemented.
func (idx *dedicatedInvertedIndex) Lookup(context.Context, string) (invertedPostingIterator, error) {
	return nil, errInvertedIndexStorageNotImplemented
}

// Stats will return term posting metadata once inline posting-list storage is implemented.
func (idx *dedicatedInvertedIndex) Stats(context.Context, string) (invertedPostingStats, error) {
	return invertedPostingStats{}, errInvertedIndexStorageNotImplemented
}
