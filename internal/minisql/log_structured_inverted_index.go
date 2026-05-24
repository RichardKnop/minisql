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
	baseIter, err := idx.base.Lookup(ctx, term)
	if err != nil {
		return nil, err
	}

	iterators := []invertedPostingIterator{baseIter}
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return nil, err
	}
	for _, segment := range meta.Segments {
		if segment.Kind != invertedSegmentKindInsert {
			continue
		}
		block, found, err := idx.lookupSegmentBlock(ctx, segment.RootPage, term)
		if err != nil {
			return nil, err
		}
		if found {
			iterators = append(iterators, &singleBlockInvertedPostingIterator{block: block, hasBlock: true})
		}
	}
	return &concatenatingInvertedPostingIterator{iterators: iterators}, nil
}

func (idx *logStructuredInvertedIndex) Stats(ctx context.Context, term string) (invertedPostingStats, error) {
	stats, err := idx.base.Stats(ctx, term)
	if err != nil {
		return invertedPostingStats{}, err
	}
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return invertedPostingStats{}, err
	}
	for _, segment := range meta.Segments {
		if segment.Kind != invertedSegmentKindInsert {
			continue
		}
		cell, found, err := idx.lookupSegmentCell(ctx, segment.RootPage, term)
		if err != nil {
			return invertedPostingStats{}, err
		}
		if found {
			stats.DocFreq += cell.DocFreq
			stats.PostingCount += cell.PostingCount
		}
	}
	return stats, nil
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
				if err := idx.freeSegmentPages(ctx, segment.RootPage); err != nil {
					return fmt.Errorf("free inverted segment root %d: %w", segment.RootPage, err)
				}
			}
		}
	}
	return idx.pager.AddFreePage(ctx, idx.rootPageIdx)
}

func (idx *logStructuredInvertedIndex) readMetaPage(ctx context.Context) (*invertedMetaPage, error) {
	page, err := idx.pager.ReadPage(ctx, idx.rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("read inverted metadata root: %w", err)
	}
	if page.InvertedMetaPage == nil {
		return nil, fmt.Errorf("inverted index %s root page %d is not a metadata page", idx.name, idx.rootPageIdx)
	}
	return page.InvertedMetaPage, nil
}

func (idx *logStructuredInvertedIndex) lookupSegmentBlock(ctx context.Context, root PageIndex, term string) (invertedPostingBlock, bool, error) {
	cell, found, err := idx.lookupSegmentCell(ctx, root, term)
	if err != nil {
		return invertedPostingBlock{}, false, err
	}
	if !found {
		return invertedPostingBlock{}, false, nil
	}
	return cell.Block, true, nil
}

func (idx *logStructuredInvertedIndex) lookupSegmentCell(ctx context.Context, root PageIndex, term string) (invertedSegmentCell, bool, error) {
	for pageIdx := root; pageIdx != 0; {
		page, err := idx.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return invertedSegmentCell{}, false, fmt.Errorf("read inverted segment page %d: %w", pageIdx, err)
		}
		if page.InvertedSegmentPage == nil {
			return invertedSegmentCell{}, false, fmt.Errorf("inverted segment page %d has unexpected page type", pageIdx)
		}
		for _, cell := range page.InvertedSegmentPage.Cells {
			if cell.Term == term {
				return cell, true, nil
			}
		}
		pageIdx = page.InvertedSegmentPage.Header.NextPage
	}
	return invertedSegmentCell{}, false, nil
}

func (idx *logStructuredInvertedIndex) freeSegmentPages(ctx context.Context, root PageIndex) error {
	for pageIdx := root; pageIdx != 0; {
		page, err := idx.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("read inverted segment page %d for free: %w", pageIdx, err)
		}
		nextPage := PageIndex(0)
		if page.InvertedSegmentPage != nil {
			nextPage = page.InvertedSegmentPage.Header.NextPage
		}
		if err := idx.pager.AddFreePage(ctx, pageIdx); err != nil {
			return err
		}
		pageIdx = nextPage
	}
	return nil
}

type concatenatingInvertedPostingIterator struct {
	iterators []invertedPostingIterator
	index     int
}

func (it *concatenatingInvertedPostingIterator) NextBlock(ctx context.Context) (invertedPostingBlock, bool, error) {
	for it.index < len(it.iterators) {
		block, ok, err := it.iterators[it.index].NextBlock(ctx)
		if err != nil {
			return invertedPostingBlock{}, false, err
		}
		if ok {
			return block, true, nil
		}
		it.index++
	}
	return invertedPostingBlock{}, false, nil
}
