package minisql

func (p *pagerImpl) ForTable(columns []Column) Pager {
	return &tablePager{
		pagerImpl: p,
		columns:   columns,
	}
}

func (p *pagerImpl) ForIndex(kind ColumnKind, keySize uint64) Pager {
	switch kind {
	case Boolean:
		return &indexPager[int8]{p}
	case Int4:
		return &indexPager[int32]{p}
	case Int8:
		return &indexPager[int64]{p}
	case Real:
		return &indexPager[float32]{p}
	case Double:
		return &indexPager[float64]{p}
	case Varchar:
		return &indexPager[string]{p}
	default:
		return nil
	}
}
