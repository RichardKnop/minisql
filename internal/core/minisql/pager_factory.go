package minisql

func (p *pagerImpl) ForTable(rowSize uint64) Pager {
	return &tablePager{
		pagerImpl: p,
		rowSize:   rowSize,
	}
}

func (p *pagerImpl) ForIndex(kind ColumnKind, keySize uint64) Pager {
	switch kind {
	case Boolean:
		return &indexPager[int8]{p, keySize}
	case Int4:
		return &indexPager[int32]{p, keySize}
	case Int8:
		return &indexPager[int64]{p, keySize}
	case Real:
		return &indexPager[float32]{p, keySize}
	case Double:
		return &indexPager[float64]{p, keySize}
	case Varchar:
		return &indexPager[string]{p, keySize}
	default:
		return nil
	}
}
