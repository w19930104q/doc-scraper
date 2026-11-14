package parse

import (
	"errors"
	"strings"
	"time"
)

// sitemapLastModLayouts lists every W3C Datetime shape the sitemap protocol
// allows, most precise first. Date-only values are midnight UTC, matching the
// protocol's note that a bare date has no time zone and is interpreted as UTC.
var sitemapLastModLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02",
}

// ErrInvalidLastMod means a sitemap lastmod value is empty or matches none of
// the W3C Datetime layouts sitemaps permit.
var ErrInvalidLastMod = errors.New("parse: sitemap lastmod is not a valid W3C Datetime value")

// ParseSitemapLastMod parses a sitemap <lastmod> value. Empty input, an
// unparseable value, or one with trailing junk fails with ErrInvalidLastMod so
// callers treat the entry conservatively (fetch the page) rather than skipping
// it on an unverifiable timestamp.
func ParseSitemapLastMod(value string) (time.Time, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return time.Time{}, ErrInvalidLastMod
	}
	for _, layout := range sitemapLastModLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, ErrInvalidLastMod
}
