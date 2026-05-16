package minisql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildMCV(t *testing.T) {
	t.Parallel()
	freq := map[string]int64{
		"open":    900,
		"closed":  80,
		"pending": 20,
	}
	mcv := buildMCV(freq, 2)
	require.Len(t, mcv, 2)
	assert.Equal(t, "open", mcv[0].Value)
	assert.Equal(t, int64(900), mcv[0].Count)
	assert.Equal(t, "closed", mcv[1].Value)
	assert.Equal(t, int64(80), mcv[1].Count)
}

func TestBuildMCV_EmptyFreq(t *testing.T) {
	t.Parallel()
	assert.Empty(t, buildMCV(nil, 50))
}

func TestBuildMCV_FewerThanN(t *testing.T) {
	t.Parallel()
	freq := map[string]int64{"a": 5, "b": 3}
	mcv := buildMCV(freq, 50)
	assert.Len(t, mcv, 2)
}

func TestSerializeParseMCV_RoundTrip(t *testing.T) {
	t.Parallel()

	input := []MCVEntry{
		{Value: "open", Count: 900},
		{Value: "a,b:c", Count: 50}, // commas and colons must be URL-encoded
		{Value: "42", Count: 10},
	}

	var sb strings.Builder
	serializeMCV(&sb, input)
	s := sb.String()
	require.NotEmpty(t, s)

	const prefix = "|mcv="
	require.True(t, strings.HasPrefix(s, prefix))
	got, err := parseMCV(s[len(prefix):])
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "open", got[0].Value)
	assert.Equal(t, int64(900), got[0].Count)
	assert.Equal(t, "a,b:c", got[1].Value)
	assert.Equal(t, int64(50), got[1].Count)
	assert.Equal(t, "42", got[2].Value)
	assert.Equal(t, int64(10), got[2].Count)
}

func TestSerializeMCV_Empty(t *testing.T) {
	t.Parallel()
	var sb strings.Builder
	serializeMCV(&sb, nil)
	assert.Empty(t, sb.String())
}

func TestParseMCV_Empty(t *testing.T) {
	t.Parallel()
	got, err := parseMCV("")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestParseIndexStats_WithMCV(t *testing.T) {
	t.Parallel()
	stat := "1000 5 |h=1,2,3|mcv=open:900,closed:80"
	s, err := parseIndexStats(stat)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), s.NEntry)
	require.Len(t, s.NDistinct, 1)
	assert.Equal(t, int64(5), s.NDistinct[0])
	require.NotNil(t, s.Hist)
	require.Len(t, s.MCV, 2)
	assert.Equal(t, "open", s.MCV[0].Value)
	assert.Equal(t, int64(900), s.MCV[0].Count)
}

func TestParseIndexStats_MCVOnly(t *testing.T) {
	t.Parallel()
	stat := "500 3|mcv=foo:400,bar:100"
	s, err := parseIndexStats(stat)
	require.NoError(t, err)
	assert.Equal(t, int64(500), s.NEntry)
	assert.Nil(t, s.Hist)
	require.Len(t, s.MCV, 2)
	assert.Equal(t, "foo", s.MCV[0].Value)
}

func TestParseIndexStats_NoMCV(t *testing.T) {
	t.Parallel()
	// Existing format without MCV must still parse.
	stat := "200 10 |h=1,10,100"
	s, err := parseIndexStats(stat)
	require.NoError(t, err)
	assert.Equal(t, int64(200), s.NEntry)
	assert.Nil(t, s.MCV)
}

func TestEstimateEqualityRows(t *testing.T) {
	t.Parallel()

	s := IndexStats{
		NEntry:    1000,
		NDistinct: []int64{5},
		MCV: []MCVEntry{
			{Value: "open", Count: 900},
			{Value: "closed", Count: 80},
		},
	}

	t.Run("mcv_hit_exact", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, int64(900), s.EstimateEqualityRows("open"))
		assert.Equal(t, int64(80), s.EstimateEqualityRows("closed"))
	})

	t.Run("mcv_miss_ndv_fallback", func(t *testing.T) {
		t.Parallel()
		// "pending" not in MCV → NEntry/NDistinct = 1000/5 = 200
		assert.Equal(t, int64(200), s.EstimateEqualityRows("pending"))
	})

	t.Run("no_ndv_returns_nentry", func(t *testing.T) {
		t.Parallel()
		empty := IndexStats{NEntry: 500}
		assert.Equal(t, int64(500), empty.EstimateEqualityRows("anything"))
	})
}

func TestShouldUseIndexForEquality(t *testing.T) {
	t.Parallel()

	stats := &IndexStats{
		NEntry:    1000,
		NDistinct: []int64{5},
		MCV: []MCVEntry{
			{Value: "open", Count: 900}, // 90% → prefer sequential
			{Value: "rare", Count: 10},  // 1%  → use index
		},
	}

	assert.False(t, shouldUseIndexForEquality(stats, "open", 1000),
		"90% selectivity should skip the index")
	assert.True(t, shouldUseIndexForEquality(stats, "rare", 1000),
		"1% selectivity should use index")

	// Missing stats → always use index.
	assert.True(t, shouldUseIndexForEquality(nil, "any", 1000))

	// Unknown table row count → always use index.
	assert.True(t, shouldUseIndexForEquality(stats, "open", 0))
	assert.True(t, shouldUseIndexForEquality(stats, "open", -1))
}
