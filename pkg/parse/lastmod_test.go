package parse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSitemapLastMod(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantUTC string // RFC3339; empty means an error is expected
	}{
		{name: "date only", value: "2024-01-15", wantUTC: "2024-01-15T00:00:00Z"},
		{name: "date and minute", value: "2024-01-15T10:30", wantUTC: "2024-01-15T10:30:00Z"},
		{name: "seconds no zone", value: "2024-01-15T10:30:45", wantUTC: "2024-01-15T10:30:45Z"},
		{name: "rfc3339 utc", value: "2024-01-15T10:30:45Z", wantUTC: "2024-01-15T10:30:45Z"},
		{name: "rfc3339 offset", value: "2024-01-15T10:30:45+02:00", wantUTC: "2024-01-15T08:30:45Z"},
		{name: "fractional offset", value: "2024-01-15T10:30:45.123+02:00", wantUTC: "2024-01-15T08:30:45.123Z"},
		{name: "nanos utc", value: "2024-01-15T10:30:45.123456789Z", wantUTC: "2024-01-15T10:30:45.123456789Z"},
		{name: "padded date", value: "2024-01-01", wantUTC: "2024-01-01T00:00:00Z"},
		{name: "surrounding whitespace tolerated", value: "  2024-01-15T10:30:45Z\n", wantUTC: "2024-01-15T10:30:45Z"},

		{name: "empty", value: ""},
		{name: "whitespace only", value: "   "},
		{name: "garbage", value: "yesterday"},
		{name: "bad date", value: "2024-13-40"},
		{name: "date with trailing junk", value: "2024-01-15 extra"},
		{name: "timestamp with trailing junk", value: "2024-01-15T10:30:45Z junk"},
		{name: "unpadded date rejected", value: "2024-1-5"},
		{name: "two digit year rejected", value: "24-01-15"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSitemapLastMod(tt.value)
			if tt.wantUTC == "" {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidLastMod)
				assert.True(t, got.IsZero())
				return
			}
			require.NoError(t, err)
			want, err := time.Parse(time.RFC3339Nano, tt.wantUTC)
			require.NoError(t, err)
			assert.True(t, got.Equal(want), "got %s want %s", got, want)
			assert.Equal(t, time.UTC, got.Location(), "values are normalized to UTC")
		})
	}
}
