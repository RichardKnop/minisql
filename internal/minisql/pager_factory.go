package minisql

import (
	"fmt"
)

// ForTable wraps the underlying pager with a table-specific unmarshaler that
// decodes leaf, internal, overflow, and free pages using the given column schema.
func (p *pagerImpl) ForTable(columns []Column) Pager {
	return &tablePager{
		pagerImpl: p,
		columns:   columns,
	}
}

// ForInvertedIndex wraps the underlying pager with the dedicated inverted-index
// unmarshaler for future entry and posting tree pages.
func (p *pagerImpl) ForInvertedIndex() Pager {
	return &invertedPager{pagerImpl: p}
}

// ForHNSWIndex wraps the underlying pager with the HNSW-specific unmarshaler
// that decodes HNSW meta and data pages.
func (p *pagerImpl) ForHNSWIndex() Pager {
	return &hnswPager{pagerImpl: p}
}

// ForIndex wraps the underlying pager with a type-parameterised index unmarshaler
// chosen from the column kind. Composite (multi-column) indexes always use
// CompositeKey; single-column indexes select the concrete key type that matches
// the column kind (int8, int32, int64, float32, float64, string, or UUIDValue).
func (p *pagerImpl) ForIndex(columns []Column, unique bool) Pager {
	if len(columns) > 1 {
		return &indexPager[CompositeKey]{p, columns, unique}
	}
	switch columns[0].Kind {
	case Boolean:
		return &indexPager[int8]{p, columns, unique}
	case Int4:
		return &indexPager[int32]{p, columns, unique}
	case Int8, Timestamp:
		return &indexPager[int64]{p, columns, unique}
	case Real:
		return &indexPager[float32]{p, columns, unique}
	case Double:
		return &indexPager[float64]{p, columns, unique}
	case Varchar:
		return &indexPager[string]{p, columns, unique}
	case UUID:
		return &indexPager[UUIDValue]{p, columns, unique}
	default:
		panic(fmt.Sprintf("unsupported index column type: %v", columns[0].Kind))
	}
}
