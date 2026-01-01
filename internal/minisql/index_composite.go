package minisql

import (
	"fmt"
)

type CompositeKey struct {
	Columns    []Column
	Values     []any
	Comparison []byte
	Storage    []byte
}

func NewCompositeKey(columns []Column, values []any) (CompositeKey, error) {
	ck := CompositeKey{
		Columns: columns,
		Values:  values,
	}
	var err error
	ck, err = ck.generateComparisonAndStorage()
	return ck, err
}

func (ck CompositeKey) generateComparisonAndStorage() (CompositeKey, error) {
	// Simple serialization for demonstration purposes
	// In a real implementation, this should handle types and nulls properly
	comparisonCapacity := 0
	storageLength := 0
	for i, aColumn := range ck.Columns {
		switch aColumn.Kind {
		case Boolean:
			comparisonCapacity += 1
			storageLength += 1
		case Int4:
			comparisonCapacity += 4
			storageLength += 4
		case Int8, Timestamp:
			comparisonCapacity += 8
			storageLength += 8
		case Real:
			comparisonCapacity += 4
			storageLength += 4
		case Double:
			comparisonCapacity += 8
			storageLength += 8
		case Varchar:
			comparisonCapacity += len(ck.Values[i].(string))
			storageLength += varcharLengthPrefixSize + len(ck.Values[i].(string))
		}
	}

	var (
		comp             = make([]byte, comparisonCapacity)
		stor             = make([]byte, storageLength)
		compIdx, storIdx = uint64(0), uint64(0)
	)
	for i, aColumn := range ck.Columns {
		switch aColumn.Kind {
		case Boolean:
			marshalBool(comp, ck.Values[i].(bool), compIdx)
			marshalBool(stor, ck.Values[i].(bool), storIdx)
			compIdx += 1
			storIdx += 1
		case Int4:
			marshalInt32(comp, ck.Values[i].(int32), compIdx)
			marshalInt32(stor, ck.Values[i].(int32), storIdx)
			compIdx += 4
			storIdx += 4
		case Int8, Timestamp:
			marshalInt64(comp, ck.Values[i].(int64), compIdx)
			marshalInt64(stor, ck.Values[i].(int64), storIdx)
			compIdx += 8
			storIdx += 8
		case Real:
			marshalFloat32(comp, ck.Values[i].(float32), compIdx)
			marshalFloat32(stor, ck.Values[i].(float32), storIdx)
			compIdx += 4
			storIdx += 4
		case Double:
			marshalFloat64(comp, ck.Values[i].(float64), compIdx)
			marshalFloat64(stor, ck.Values[i].(float64), storIdx)
			compIdx += 8
			storIdx += 8
		case Varchar:
			tp, ok := ck.Values[i].(TextPointer)
			if !ok {
				return CompositeKey{}, fmt.Errorf("could not cast value for column %s to text pointer", aColumn.Name)
			}

			copy(comp[compIdx:compIdx+uint64(tp.Length)], tp.Data)
			compIdx += uint64(tp.Length)

			marshalUint32(stor, tp.Length, storIdx)
			storIdx += 4
			copy(stor[storIdx:storIdx+uint64(tp.Length)], tp.Data)
			storIdx += uint64(tp.Length)
		}
	}
	return ck, nil
}
