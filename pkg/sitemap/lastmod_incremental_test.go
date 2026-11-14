package sitemap

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/Sriram-PR/doc-scraper/v2/pkg/fetch"
	"github.com/Sriram-PR/doc-scraper/v2/pkg/queue"
)

// fixedNow returns a frozen wall clock for deterministic future-lastmod tests.
func fixedNow(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// newIncrementalProcessor builds a processor with lastmod skipping armed.
func newIncrementalProcessor(t *testing.T, body string, lastSuccess, now time.Time) (*SitemapProcessor, chan string, *queue.ThreadSafePriorityQueue, *sync.WaitGroup, *mockPageStore) {
	t.Helper()
	log := discardLogger()
	var wg sync.WaitGroup
	pq := queue.NewThreadSafePriorityQueue(log, &wg)
	store := newMockPageStore()
	sitemapQueue := make(chan string, 10)
	sp := NewSitemapProcessor(
		sitemapQueue, pq, store, newMockFetcher(body), fetch.NewRateLimiter(0, log),
		semaphore.NewWeighted(10), fetch.NewHostSemaphorePool(10, log),
		nil, defaultSiteCfg(), defaultAppCfg(), log, &wg,
	)
	sp.ConfigureIncremental(lastSuccess, nil)
	sp.now = fixedNow(now)
	return sp, sitemapQueue, pq, &wg, store
}

// urlsetWith builds a urlset sitemap body from loc/lastmod pairs.
func urlsetWith(entries ...[2]string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`)
	for _, e := range entries {
		b.WriteString(`<url><loc>https://example.com/docs/` + e[0] + `</loc>`)
		if e[1] != "" {
			b.WriteString(`<lastmod>` + e[1] + `</lastmod>`)
		}
		b.WriteString(`</url>`)
	}
	b.WriteString(`</urlset>`)
	return b.String()
}

func TestShouldSkipForLastMod(t *testing.T) {
	cutoff := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	now := time.Date(2025, 6, 10, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		lastmod string
		want    bool
	}{
		{"older than cutoff is skipped", "2025-05-01T00:00:00Z", true},
		{"date only older is skipped", "2025-05-01", true},
		{"equal to cutoff is skipped", "2025-06-01T12:00:00Z", true},
		{"newer than cutoff is fetched", "2025-06-05T00:00:00Z", false},
		{"missing lastmod is fetched", "", false},
		{"malformed lastmod is fetched", "not-a-date", false},
		{"truncated timestamp is fetched", "2025-05-01T10", false},
		{"future lastmod is fetched", "2025-07-01T00:00:00Z", false},
		{"future date-only is fetched", "2026-01-01", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp, _, _, _, _ := newIncrementalProcessor(t, "", cutoff, now)
			got := sp.shouldSkipForLastMod(tt.lastmod, "https://example.com/docs/x", discardLogger())
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIncrementalDisabledNeverSkips(t *testing.T) {
	log := discardLogger()
	var wg sync.WaitGroup
	pq := queue.NewThreadSafePriorityQueue(log, &wg)
	store := newMockPageStore()
	sitemapQueue := make(chan string, 10)
	body := urlsetWith([2]string{"a.html", "2020-01-01"})
	sp := NewSitemapProcessor(
		sitemapQueue, pq, store, newMockFetcher(body), fetch.NewRateLimiter(0, log),
		semaphore.NewWeighted(10), fetch.NewHostSemaphorePool(10, log),
		nil, defaultSiteCfg(), defaultAppCfg(), log, &wg,
	)
	// Not configured: even an ancient lastmod must be queued normally.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sp.Start(ctx)

	wg.Add(1)
	sitemapQueue <- "https://example.com/sitemap.xml"
	waitForPQLen(t, pq, 1, 5*time.Second)
	items := drainPQAndBalance(pq, &wg)
	wg.Wait()

	require.Len(t, items, 1)
	assert.Zero(t, sp.UnchangedCount())
}

func TestIncrementalNoCutoffNeverSkips(t *testing.T) {
	// A zero lastSuccess (no crawl history) makes ConfigureIncremental inert.
	body := urlsetWith([2]string{"a.html", "2020-01-01"})
	sp, sitemapQueue, pq, wg, _ := newIncrementalProcessor(t, body, time.Time{}, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sp.Start(ctx)

	wg.Add(1)
	sitemapQueue <- "https://example.com/sitemap.xml"
	waitForPQLen(t, pq, 1, 5*time.Second)
	items := drainPQAndBalance(pq, wg)
	wg.Wait()

	require.Len(t, items, 1)
	assert.Zero(t, sp.UnchangedCount())
}

func TestURLSetLastModMixedSkipAndFetch(t *testing.T) {
	cutoff := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	now := time.Date(2025, 6, 10, 12, 0, 0, 0, time.UTC)
	body := urlsetWith(
		[2]string{"old.html", "2025-05-01T00:00:00Z"},    // skip
		[2]string{"fresh.html", "2025-06-08T00:00:00Z"},  // fetch
		[2]string{"nolastmod.html", ""},                  // fetch
		[2]string{"baddate.html", "yesterday"},           // fetch
		[2]string{"future.html", "2026-01-01T00:00:00Z"}, // fetch
		[2]string{"dateonly.html", "2025-05-15"},         // skip
	)
	sp, sitemapQueue, pq, wg, store := newIncrementalProcessor(t, body, cutoff, now)
	var skipped []string
	var mu sync.Mutex
	sp.onUnchanged = func(normalized string) {
		mu.Lock()
		skipped = append(skipped, normalized)
		mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sp.Start(ctx)

	wg.Add(1)
	sitemapQueue <- "https://example.com/sitemap.xml"
	waitForPQLen(t, pq, 4, 5*time.Second)
	items := drainPQAndBalance(pq, wg)
	wg.Wait()

	queued := make(map[string]bool, len(items))
	for _, it := range items {
		queued[it.URL] = true
	}
	assert.Len(t, items, 4)
	for _, want := range []string{
		"https://example.com/docs/fresh.html",
		"https://example.com/docs/nolastmod.html",
		"https://example.com/docs/baddate.html",
		"https://example.com/docs/future.html",
	} {
		assert.True(t, queued[want], "must be queued: %s", want)
	}
	assert.Equal(t, 4, store.visitedCount(), "skipped URLs are never marked visited")

	require.Len(t, skipped, 2)
	assert.Contains(t, skipped, "https://example.com/docs/old.html")
	assert.Contains(t, skipped, "https://example.com/docs/dateonly.html")
	assert.EqualValues(t, 2, sp.UnchangedCount())
}

func TestURLSetAllSkippedBalancesWaitGroup(t *testing.T) {
	cutoff := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	body := urlsetWith(
		[2]string{"a.html", "2025-01-01"},
		[2]string{"b.html", "2025-02-01"},
		[2]string{"c.html", "2025-03-01T10:00:00Z"},
	)
	sp, sitemapQueue, pq, wg, store := newIncrementalProcessor(t, body, cutoff, cutoff.Add(24*time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sp.Start(ctx)

	wg.Add(1)
	sitemapQueue <- "https://example.com/sitemap.xml"
	waitDone(t, wg, 5*time.Second)

	assert.Empty(t, drainPQ(pq), "no page tasks are queued")
	assert.Zero(t, store.visitedCount())
	assert.EqualValues(t, 3, sp.UnchangedCount())
}

// TestURLSetLastModThroughSitemapIndex covers the recursive requirement: the
// lastmod rule is applied by the urlset handler reached through an index, and
// index-level lastmod values are parsed but never cause a child sitemap to be
// skipped.
func TestURLSetLastModThroughSitemapIndex(t *testing.T) {
	const ns = `xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"`
	cutoff := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	nestedA := urlsetWith(
		[2]string{"old.html", "2025-01-01T00:00:00Z"},
		[2]string{"fresh.html", "2025-06-09T00:00:00Z"},
	)
	nestedB := urlsetWith(
		[2]string{"other.html", "2025-02-01T00:00:00Z"}, // old -> skipped
	)
	indexBody := `<?xml version="1.0"?><sitemapindex ` + ns + `>` +
		`<sitemap><loc>https://example.com/docs/sitemap-0.xml</loc><lastmod>2024-01-01</lastmod></sitemap>` +
		`<sitemap><loc>https://example.com/docs/sitemap-1.xml</loc><lastmod>garbage</lastmod></sitemap>` +
		`</sitemapindex>`
	routes := map[string]string{
		"https://example.com/docs/index.xml":     indexBody,
		"https://example.com/docs/sitemap-0.xml": nestedA,
		"https://example.com/docs/sitemap-1.xml": nestedB,
	}

	log := discardLogger()
	var wg sync.WaitGroup
	pq := queue.NewThreadSafePriorityQueue(log, &wg)
	store := newMockPageStore()
	sitemapQueue := make(chan string, 10)
	sp := NewSitemapProcessor(
		sitemapQueue, pq, store, &routeFetcher{routes: routes}, fetch.NewRateLimiter(0, log),
		semaphore.NewWeighted(4), fetch.NewHostSemaphorePool(4, log),
		nil, defaultSiteCfg(), defaultAppCfg(), log, &wg,
	)
	sp.ConfigureIncremental(cutoff, nil)
	sp.now = fixedNow(cutoff.Add(24 * time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sp.Start(ctx)

	wg.Add(1)
	sitemapQueue <- "https://example.com/docs/index.xml"
	waitForPQLen(t, pq, 1, 5*time.Second) // only fresh.html is queued; old.html and other.html skip
	items := drainPQAndBalance(pq, &wg)
	wg.Wait()

	require.Len(t, items, 1)
	assert.Equal(t, "https://example.com/docs/fresh.html", items[0].URL)
	assert.EqualValues(t, 2, sp.UnchangedCount(), "old pages reached through nested urlsets are skipped")
}

// TestLastModSkipRespectsInScope keeps the skip rule behind the existing scope
// filters: an out-of-scope URL is neither queued nor counted as unchanged.
func TestLastModSkipRespectsInScope(t *testing.T) {
	cutoff := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	body := `<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
		<url><loc>https://example.com/docs/in.html</loc><lastmod>2025-01-01</lastmod></url>
		<url><loc>https://example.com/blog/out.html</loc><lastmod>2025-01-01</lastmod></url>
	</urlset>`
	sp, sitemapQueue, pq, wg, store := newIncrementalProcessor(t, body, cutoff, cutoff.Add(24*time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sp.Start(ctx)

	wg.Add(1)
	sitemapQueue <- "https://example.com/sitemap.xml"
	waitDone(t, wg, 5*time.Second)

	assert.Empty(t, drainPQ(pq))
	assert.Zero(t, store.visitedCount())
	assert.EqualValues(t, 1, sp.UnchangedCount(), "only the in-scope URL is counted")
}

// TestConfigureIncrementalUsesUTCCutoff normalizes an offset cutoff so the
// comparison against UTC-normalized lastmod values is zone-independent.
func TestConfigureIncrementalUsesUTCCutoff(t *testing.T) {
	sp, _, _, _, _ := newIncrementalProcessor(t, "", time.Now(), time.Now())
	offset := time.Date(2025, 6, 1, 20, 0, 0, 0, time.FixedZone("+08", 8*3600)) // 12:00 UTC
	sp.ConfigureIncremental(offset, nil)
	require.False(t, sp.lastSuccess.IsZero())
	_, offsetSec := sp.lastSuccess.Zone()
	assert.Equal(t, 0, offsetSec)
	assert.Equal(t, "2025-06-01T12:00:00Z", sp.lastSuccess.Format(time.RFC3339))
}

// TestFutureCutoffSkipsNothing documents the conservative behavior when the
// whole clock is behind the cutoff: normal lastmod values still compare
// correctly; only genuinely future-dated entries fall through to a fetch.
func TestFutureCutoffSkipsNothing(t *testing.T) {
	cutoff := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	now := time.Date(2025, 6, 10, 12, 0, 0, 0, time.UTC)
	sp, _, _, _, _ := newIncrementalProcessor(t, "", cutoff, now)
	assert.False(t, sp.shouldSkipForLastMod("2025-06-09T00:00:00Z", "u", discardLogger()))
	assert.True(t, sp.shouldSkipForLastMod("2025-05-09T00:00:00Z", "u", discardLogger()))
}
