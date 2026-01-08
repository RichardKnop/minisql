package minisql

import (
	"fmt"
)

func (p *pagerImpl) ForTable(columns []Column) Pager {
	return &tablePager{
		pagerImpl: p,
		columns:   columns,
	}
}

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
	default:
		panic(fmt.Sprintf("unsupported index column type: %v", columns[0].Kind))
	}
}
