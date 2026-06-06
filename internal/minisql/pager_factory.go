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
//
// Text, JSON, and Vector columns are not supported for B-tree secondary indexes:
//   - TEXT / JSON: use CREATE FULLTEXT INDEX or CREATE INVERTED INDEX instead.
//   - VECTOR: use CREATE HNSW INDEX instead.
func (p *pagerImpl) ForIndex(columns []Column, unique bool) (Pager, error) {
	if len(columns) > 1 {
		for _, col := range columns {
			switch col.Kind {
			case Text, JSON:
				return nil, fmt.Errorf(
					"column %q (%s) cannot be used in a B-tree secondary index: "+
						"TEXT and JSON columns support only FULLTEXT INDEX or INVERTED INDEX",
					col.Name, col.Kind)
			case Vector:
				return nil, fmt.Errorf(
					"column %q (%s) cannot be used in a B-tree secondary index: "+
						"VECTOR columns support only HNSW INDEX",
					col.Name, col.Kind)
			}
		}
		return &indexPager[CompositeKey]{p, columns, unique}, nil
	}
	switch columns[0].Kind {
	case Boolean:
		return &indexPager[int8]{p, columns, unique}, nil
	case Int4:
		return &indexPager[int32]{p, columns, unique}, nil
	case Int8, Timestamp:
		return &indexPager[int64]{p, columns, unique}, nil
	case Real:
		return &indexPager[float32]{p, columns, unique}, nil
	case Double:
		return &indexPager[float64]{p, columns, unique}, nil
	case Varchar:
		return &indexPager[string]{p, columns, unique}, nil
	case UUID:
		return &indexPager[UUIDValue]{p, columns, unique}, nil
	case Text, JSON:
		return nil, fmt.Errorf(
			"column %q (%s) cannot be used in a B-tree secondary index: "+
				"TEXT and JSON columns support only FULLTEXT INDEX or INVERTED INDEX",
			columns[0].Name, columns[0].Kind)
	case Vector:
		return nil, fmt.Errorf(
			"column %q (%s) cannot be used in a B-tree secondary index: "+
				"VECTOR columns support only HNSW INDEX",
			columns[0].Name, columns[0].Kind)
	default:
		return nil, fmt.Errorf("unsupported index column type %q", columns[0].Kind)
	}
}
