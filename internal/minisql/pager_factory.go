package minisql

func (p *pagerImpl) ForTable(columns []Column) Pager {
	return &tablePager{
		pagerImpl: p,
		columns:   columns,
	}
}

func (p *pagerImpl) ForIndex(columns []Column, unique bool) Pager {
	if len(columns) > 1 {
		return nil // Composite indexes not supported yet
	}
	switch columns[0].Kind {
	case Boolean:
		return &indexPager[int8]{p, unique}
	case Int4:
		return &indexPager[int32]{p, unique}
	case Int8, Timestamp:
		return &indexPager[int64]{p, unique}
	case Real:
		return &indexPager[float32]{p, unique}
	case Double:
		return &indexPager[float64]{p, unique}
	case Varchar:
		return &indexPager[string]{p, unique}
	default:
		return nil
	}
}
