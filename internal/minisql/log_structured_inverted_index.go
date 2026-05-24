package minisql

import (
	"context"
	"fmt"
)

type logStructuredInvertedIndex struct {
	pager       TxPager
	rootPageIdx PageIndex
	base        *dedicatedInvertedIndex
	name        string
}

var _ invertedIndex = (*logStructuredInvertedIndex)(nil)
var _ invertedBatchApplier = (*logStructuredInvertedIndex)(nil)

func OpenInvertedIndex(ctx context.Context, name string, mode invertedIndexPostingMode, pager TxPager, rootPageIdx PageIndex) (invertedIndex, error) {
	page, err := pager.ReadPage(ctx, rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("read inverted index root: %w", err)
	}
	if page.InvertedMetaPage != nil {
		return newLogStructuredInvertedIndexFromMeta(name, mode, pager, rootPageIdx, page.InvertedMetaPage)
	}
	if page.InvertedEntryPage != nil {
		return NewDedicatedInvertedIndex(name, mode, pager, rootPageIdx)
	}
	return nil, fmt.Errorf("inverted index %s root page %d is neither metadata nor entry page", name, rootPageIdx)
}

func NewLogStructuredInvertedIndex(
	ctx context.Context,
	name string,
	mode invertedIndexPostingMode,
	pager TxPager,
	rootPageIdx, baseRootPageIdx PageIndex,
) (*logStructuredInvertedIndex, error) {
	if mode != invertedIndexPostingModeRowIDs && mode != invertedIndexPostingModePositions {
		return nil, fmt.Errorf("unknown inverted index posting mode %d", mode)
	}

	metaPage, err := pager.ModifyPage(ctx, rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("modify inverted metadata root: %w", err)
	}
	metaPage.Clear()
	metaPage.InvertedMetaPage = NewInvertedMetaPage(mode, baseRootPageIdx)

	base, err := NewDedicatedInvertedIndex(name, mode, pager, baseRootPageIdx)
	if err != nil {
		return nil, err
	}
	if err := base.InitRootPage(ctx); err != nil {
		return nil, err
	}
	return &logStructuredInvertedIndex{
		name:        name,
		pager:       pager,
		rootPageIdx: rootPageIdx,
		base:        base,
	}, nil
}

func newLogStructuredInvertedIndexFromMeta(
	name string,
	mode invertedIndexPostingMode,
	pager TxPager,
	rootPageIdx PageIndex,
	meta *invertedMetaPage,
) (*logStructuredInvertedIndex, error) {
	if meta.Mode != mode {
		return nil, fmt.Errorf("inverted index %s metadata uses posting mode %d, expected %d", name, meta.Mode, mode)
	}
	base, err := NewDedicatedInvertedIndex(name, mode, pager, meta.BaseRoot)
	if err != nil {
		return nil, err
	}
	return &logStructuredInvertedIndex{
		name:        name,
		pager:       pager,
		rootPageIdx: rootPageIdx,
		base:        base,
	}, nil
}

func (idx *logStructuredInvertedIndex) GetRootPageIdx() PageIndex {
	return idx.rootPageIdx
}

func (idx *logStructuredInvertedIndex) Mode() invertedIndexPostingMode {
	return idx.base.Mode()
}

func (idx *logStructuredInvertedIndex) Insert(ctx context.Context, term string, posting invertedPosting) error {
	return idx.base.Insert(ctx, term, posting)
}

func (idx *logStructuredInvertedIndex) InsertMany(ctx context.Context, term string, postings []invertedPosting) error {
	return idx.base.InsertMany(ctx, term, postings)
}

func (idx *logStructuredInvertedIndex) Replace(ctx context.Context, term string, oldPosting, newPosting invertedPosting) error {
	return idx.base.Replace(ctx, term, oldPosting, newPosting)
}

func (idx *logStructuredInvertedIndex) Delete(ctx context.Context, term string, posting invertedPosting) error {
	return idx.base.Delete(ctx, term, posting)
}

func (idx *logStructuredInvertedIndex) ApplyBatch(ctx context.Context, batch invertedIndexMutationBatch) error {
	return applyInvertedIndexMutationBatchLegacy(ctx, idx.base, batch)
}

func (idx *logStructuredInvertedIndex) Lookup(ctx context.Context, term string) (invertedPostingIterator, error) {
	return idx.base.Lookup(ctx, term)
}

func (idx *logStructuredInvertedIndex) Stats(ctx context.Context, term string) (invertedPostingStats, error) {
	return idx.base.Stats(ctx, term)
}

func (idx *logStructuredInvertedIndex) FreeAll(ctx context.Context) error {
	if err := idx.base.FreeAll(ctx); err != nil {
		return err
	}
	page, err := idx.pager.ReadPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("read inverted metadata root for free: %w", err)
	}
	if page.InvertedMetaPage != nil {
		for _, segment := range page.InvertedMetaPage.Segments {
			if segment.RootPage != 0 {
				if err := idx.pager.AddFreePage(ctx, segment.RootPage); err != nil {
					return fmt.Errorf("free inverted segment root %d: %w", segment.RootPage, err)
				}
			}
		}
	}
	return idx.pager.AddFreePage(ctx, idx.rootPageIdx)
}
