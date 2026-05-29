package minisql

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// VectorPointer is stored in the leaf cell for a VECTOR(n) column.
// The 8-byte inline representation holds the dimension count and the first
// overflow page index; actual float32 data lives on overflow pages.
type VectorPointer struct {
	Data      []float32
	Dims      uint32
	FirstPage PageIndex
}

// Size returns the fixed 8-byte inline serialised size of the pointer.
// (4 bytes for Dims + 4 bytes for FirstPage)
func (vp VectorPointer) Size() uint64 {
	return 8
}

// Marshal writes the VectorPointer (8 bytes) into buf at byte offset i.
func (vp *VectorPointer) Marshal(buf []byte, i uint64) {
	marshalUint32(buf, vp.Dims, i)
	marshalUint32(buf, uint32(vp.FirstPage), i+4)
}

// Unmarshal reads the VectorPointer from buf at byte offset i.
// Data is not populated; call readOverflow to load the float32 values.
func (vp *VectorPointer) Unmarshal(buf []byte, i uint64) {
	vp.Dims = unmarshalUint32(buf, i)
	vp.FirstPage = PageIndex(unmarshalUint32(buf, i+4))
}

// ParseVectorLiteral parses a bracket-delimited float list like "[0.1, 0.2, 0.3]"
// into a VectorPointer with Data populated and FirstPage = 0.
func ParseVectorLiteral(s string) (VectorPointer, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return VectorPointer{}, fmt.Errorf("vector literal must be enclosed in brackets, got: %q", s)
	}
	s = s[1 : len(s)-1]
	parts := strings.Split(s, ",")
	data := make([]float32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		f, err := strconv.ParseFloat(p, 32)
		if err != nil {
			return VectorPointer{}, fmt.Errorf("invalid vector component %q: %w", p, err)
		}
		data = append(data, float32(f))
	}
	return VectorPointer{
		Dims: uint32(len(data)),
		Data: data,
	}, nil
}

// FormatVector returns the string representation "[v0, v1, ...]" of a VectorPointer.
func FormatVector(vp VectorPointer) string {
	if len(vp.Data) == 0 {
		return "[]"
	}
	var sb strings.Builder
	sb.WriteByte('[')
	for i, f := range vp.Data {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(strconv.FormatFloat(float64(f), 'f', -1, 32))
	}
	sb.WriteByte(']')
	return sb.String()
}

// toVectorPointer converts v to a VectorPointer.
// Accepts VectorPointer (pass-through), TextPointer, or string (parsed as a vector literal).
func toVectorPointer(v any) (VectorPointer, error) {
	switch vp := v.(type) {
	case VectorPointer:
		return vp, nil
	case TextPointer:
		return ParseVectorLiteral(vp.String())
	case string:
		return ParseVectorLiteral(vp)
	default:
		return VectorPointer{}, fmt.Errorf("cannot convert %T to vector", v)
	}
}

// L2Distance computes the Euclidean distance between two vectors.
func L2Distance(a, b VectorPointer) (float64, error) {
	if a.Dims != b.Dims {
		return 0, fmt.Errorf("VEC_L2: dimension mismatch: %d vs %d", a.Dims, b.Dims)
	}
	var sum float64
	for i := range a.Data {
		d := float64(a.Data[i]) - float64(b.Data[i])
		sum += d * d
	}
	return math.Sqrt(sum), nil
}

// CosineDistance computes the cosine distance (1 − cosine_similarity) between two vectors.
func CosineDistance(a, b VectorPointer) (float64, error) {
	if a.Dims != b.Dims {
		return 0, fmt.Errorf("VEC_COSINE: dimension mismatch: %d vs %d", a.Dims, b.Dims)
	}
	var dot, normA, normB float64
	for i := range a.Data {
		ai := float64(a.Data[i])
		bi := float64(b.Data[i])
		dot += ai * bi
		normA += ai * ai
		normB += bi * bi
	}
	if normA == 0 || normB == 0 {
		return 0, fmt.Errorf("VEC_COSINE: zero-length vector")
	}
	return 1 - dot/(math.Sqrt(normA)*math.Sqrt(normB)), nil
}

// storeOverflowVectors writes float32 data to overflow pages for all Vector columns in r.
func (r Row) storeOverflowVectors(ctx context.Context, pager TxPager) (Row, error) {
	for i, col := range r.Columns {
		if !col.Kind.IsVector() {
			continue
		}
		value := r.Values[i]
		if !value.Valid {
			continue
		}
		vp, ok := value.Value.(VectorPointer)
		if !ok {
			return r, fmt.Errorf("expected VectorPointer value for vector column %s", col.Name)
		}
		if err := vp.storeOverflow(ctx, pager); err != nil {
			return r, err
		}
		r.Values[i] = OptionalValue{Valid: true, Value: vp}
	}
	return r, nil
}

func (vp *VectorPointer) storeOverflow(ctx context.Context, pager TxPager) error {
	if vp.Dims == 0 {
		return nil
	}

	rawBytes := make([]byte, vp.Dims*4)
	for i, f := range vp.Data {
		marshalFloat32(rawBytes, f, uint64(i)*4)
	}

	totalBytes := uint32(len(rawBytes))
	numPages := (totalBytes + MaxOverflowPageData - 1) / MaxOverflowPageData
	remaining := totalBytes
	offset := uint32(0)

	var previousPage *Page
	for i := uint32(0); i < numPages; i++ {
		freePage, err := pager.GetFreePage(ctx)
		if err != nil {
			return fmt.Errorf("allocate vector overflow page: %w", err)
		}
		if i == 0 {
			vp.FirstPage = freePage.Index
		}
		dataSize := min(remaining, MaxOverflowPageData)
		remaining -= dataSize
		freePage.OverflowPage = &OverflowPage{
			Header: OverflowPageHeader{
				DataSize: dataSize,
			},
			Data: rawBytes[offset : offset+dataSize],
		}
		offset += dataSize
		if previousPage != nil {
			previousPage.OverflowPage.Header.NextPage = freePage.Index
		}
		previousPage = freePage
	}
	return nil
}

// readOverflowVectors reads float32 data from overflow pages for all Vector columns in r.
func (r Row) readOverflowVectors(ctx context.Context, pager TxPager) (Row, error) {
	for i, col := range r.Columns {
		if !col.Kind.IsVector() {
			continue
		}
		if i >= len(r.Values) || !r.Values[i].Valid {
			continue
		}
		vp, ok := r.Values[i].Value.(VectorPointer)
		if !ok {
			return Row{}, fmt.Errorf("expected VectorPointer value for vector column %s", col.Name)
		}
		if vp.Dims == 0 {
			continue
		}
		if pager == nil {
			return Row{}, fmt.Errorf("vector overflow column %d requires a pager", i)
		}
		vp, err := vp.readOverflow(ctx, pager)
		if err != nil {
			return Row{}, err
		}
		r.Values[i] = OptionalValue{Value: vp, Valid: true}
	}
	return r, nil
}

func (vp VectorPointer) readOverflow(ctx context.Context, pager TxPager) (VectorPointer, error) {
	if vp.Dims == 0 {
		return vp, nil
	}

	totalBytes := vp.Dims * 4
	rawBytes := make([]byte, 0, totalBytes)
	currentPageIdx := vp.FirstPage
	remaining := totalBytes

	for remaining > 0 {
		overflowPage, err := pager.ReadPage(ctx, currentPageIdx)
		if err != nil {
			return VectorPointer{}, fmt.Errorf("read vector overflow page %d: %w", currentPageIdx, err)
		}
		if overflowPage.OverflowPage == nil {
			return VectorPointer{}, fmt.Errorf("page %d is not an overflow page", currentPageIdx)
		}
		dataSize := min(remaining, overflowPage.OverflowPage.Header.DataSize)
		rawBytes = append(rawBytes, overflowPage.OverflowPage.Data[:dataSize]...)
		remaining -= dataSize
		currentPageIdx = overflowPage.OverflowPage.Header.NextPage
	}

	vp.Data = make([]float32, vp.Dims)
	for i := uint32(0); i < vp.Dims; i++ {
		vp.Data[i] = unmarshalFloat32(rawBytes, uint64(i)*4)
	}
	return vp, nil
}
