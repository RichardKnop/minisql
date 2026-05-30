package minisql

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	hnswMetaFormatVersion byte = 1

	// hnswNoEntryPoint is the sentinel value stored in hnswMetaPage.EntryPoint
	// when the graph has no nodes.
	hnswNoEntryPoint uint64 = math.MaxUint64

	// hnswMetaHeaderSize is the fixed byte count of a serialised hnswMetaPage.
	// Layout: type(1) + version(1) + M(2) + efConstruction(4) + entryPoint(8)
	//         + entryLevel(1) + nodeCount(4) + firstDataPage(4) = 25
	hnswMetaHeaderSize = 25

	// hnswDataPageHeaderSize is the fixed byte count of an hnswDataPage header.
	// Layout: type(1) + next_page(4) + node_count(2) = 7
	hnswDataPageHeaderSize = 7
)

// hnswMetaPage stores HNSW graph metadata and is written to the root page of
// the HNSW index.  EntryPoint == hnswNoEntryPoint signals an empty graph.
type hnswMetaPage struct {
	M              uint16
	EfConstruction uint32
	EntryPoint     uint64 // RowID of the graph entry point; hnswNoEntryPoint = empty
	EntryLevel     uint8  // highest layer of the entry-point node
	NodeCount      uint32
	FirstDataPage  uint32 // PageIndex of the first hnswDataPage; 0 = no data pages
}

// Marshal encodes the meta page into buf.  buf must be at least hnswMetaHeaderSize bytes.
func (p *hnswMetaPage) Marshal(buf []byte) error {
	if len(buf) < hnswMetaHeaderSize {
		return fmt.Errorf("HNSW meta marshal: buffer too small (%d < %d)", len(buf), hnswMetaHeaderSize)
	}
	buf[0] = PageTypeHNSWMeta
	buf[1] = hnswMetaFormatVersion
	binary.BigEndian.PutUint16(buf[2:4], p.M)
	binary.BigEndian.PutUint32(buf[4:8], p.EfConstruction)
	binary.BigEndian.PutUint64(buf[8:16], p.EntryPoint)
	buf[16] = p.EntryLevel
	binary.BigEndian.PutUint32(buf[17:21], p.NodeCount)
	binary.BigEndian.PutUint32(buf[21:25], p.FirstDataPage)
	return nil
}

// Unmarshal decodes a meta page from buf.
func (p *hnswMetaPage) Unmarshal(buf []byte) error {
	if len(buf) < hnswMetaHeaderSize {
		return fmt.Errorf("HNSW meta unmarshal: buffer too small (%d < %d)", len(buf), hnswMetaHeaderSize)
	}
	if buf[0] != PageTypeHNSWMeta {
		return fmt.Errorf("HNSW meta unmarshal: unexpected page type %d", buf[0])
	}
	p.M = binary.BigEndian.Uint16(buf[2:4])
	p.EfConstruction = binary.BigEndian.Uint32(buf[4:8])
	p.EntryPoint = binary.BigEndian.Uint64(buf[8:16])
	p.EntryLevel = buf[16]
	p.NodeCount = binary.BigEndian.Uint32(buf[17:21])
	p.FirstDataPage = binary.BigEndian.Uint32(buf[21:25])
	return nil
}

// hnswDataPage stores a chain of packed HNSW node records.  Multiple pages are
// linked via NextPage to form the full node list.
type hnswDataPage struct {
	NextPage uint32 // PageIndex of the next data page; 0 = last page
	Nodes    []hnswNodeRecord
}

// hnswNodeRecord is the in-memory form of one HNSW graph node as stored on an
// hnswDataPage.  Neighbors[l] holds the neighbor RowIDs at layer l.
type hnswNodeRecord struct {
	RowID     uint64
	Neighbors [][]uint64 // Neighbors[l] = neighbor RowIDs at layer l; len == Level+1
}

// nodeRecordSize returns the serialised byte size of one hnswNodeRecord.
// Layout: rowID(8) + level(1) + per-layer: count(2) + count*8
func nodeRecordSize(n hnswNodeRecord) int {
	size := 9
	for _, layer := range n.Neighbors {
		size += 2 + len(layer)*8
	}
	return size
}

// Marshal serialises the data page into buf.
func (p *hnswDataPage) Marshal(buf []byte) error {
	if len(buf) < hnswDataPageHeaderSize {
		return fmt.Errorf("HNSW data marshal: buffer too small (%d)", len(buf))
	}
	buf[0] = PageTypeHNSWData
	binary.BigEndian.PutUint32(buf[1:5], p.NextPage)
	binary.BigEndian.PutUint16(buf[5:7], uint16(len(p.Nodes)))
	off := hnswDataPageHeaderSize
	for _, node := range p.Nodes {
		need := nodeRecordSize(node)
		if off+need > len(buf) {
			return fmt.Errorf("HNSW data marshal: node record overflows page buffer (%d + %d > %d)", off, need, len(buf))
		}
		binary.BigEndian.PutUint64(buf[off:off+8], node.RowID)
		off += 8
		level := len(node.Neighbors) - 1
		if level < 0 {
			level = 0
		}
		buf[off] = byte(level)
		off++
		for _, neighbors := range node.Neighbors {
			binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(neighbors)))
			off += 2
			for _, nb := range neighbors {
				binary.BigEndian.PutUint64(buf[off:off+8], nb)
				off += 8
			}
		}
	}
	return nil
}

// Unmarshal deserialises an hnswDataPage from buf.
func (p *hnswDataPage) Unmarshal(buf []byte) error {
	if len(buf) < hnswDataPageHeaderSize {
		return fmt.Errorf("HNSW data unmarshal: buffer too small (%d)", len(buf))
	}
	if buf[0] != PageTypeHNSWData {
		return fmt.Errorf("HNSW data unmarshal: unexpected page type %d", buf[0])
	}
	p.NextPage = binary.BigEndian.Uint32(buf[1:5])
	nodeCount := int(binary.BigEndian.Uint16(buf[5:7]))
	off := hnswDataPageHeaderSize
	p.Nodes = make([]hnswNodeRecord, 0, nodeCount)
	for range nodeCount {
		if off+9 > len(buf) {
			return fmt.Errorf("HNSW data unmarshal: unexpected end of buffer at offset %d", off)
		}
		var node hnswNodeRecord
		node.RowID = binary.BigEndian.Uint64(buf[off : off+8])
		off += 8
		level := int(buf[off])
		off++
		node.Neighbors = make([][]uint64, level+1)
		for l := range level + 1 {
			if off+2 > len(buf) {
				return fmt.Errorf("HNSW data unmarshal: truncated layer count at offset %d", off)
			}
			count := int(binary.BigEndian.Uint16(buf[off : off+2]))
			off += 2
			if off+count*8 > len(buf) {
				return fmt.Errorf("HNSW data unmarshal: truncated neighbor list at offset %d", off)
			}
			neighbors := make([]uint64, count)
			for i := range count {
				neighbors[i] = binary.BigEndian.Uint64(buf[off : off+8])
				off += 8
			}
			node.Neighbors[l] = neighbors
		}
		p.Nodes = append(p.Nodes, node)
	}
	return nil
}

// clone returns a deep copy of the data page.
func (p *hnswDataPage) clone() *hnswDataPage {
	c := &hnswDataPage{NextPage: p.NextPage, Nodes: make([]hnswNodeRecord, len(p.Nodes))}
	for i, node := range p.Nodes {
		layers := make([][]uint64, len(node.Neighbors))
		for l, nb := range node.Neighbors {
			layers[l] = append([]uint64(nil), nb...)
		}
		c.Nodes[i] = hnswNodeRecord{RowID: node.RowID, Neighbors: layers}
	}
	return c
}
