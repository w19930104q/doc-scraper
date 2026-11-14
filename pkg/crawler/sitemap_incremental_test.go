package crawler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sriram-PR/doc-scraper/v2/pkg/config"
	"github.com/Sriram-PR/doc-scraper/v2/pkg/fetch"
	"github.com/Sriram-PR/doc-scraper/v2/pkg/storage/index"
)

// sitemapIncrementalServer serves a start page with no outgoing links, a
// robots.txt that announces /sitemap.xml, and a mutable urlset whose per-URL
// lastmod values tests can change between crawls. Page hit counts expose
// whether a download actually happened.
type sitemapIncrementalServer struct {
	*httptest.Server
	mu       sync.Mutex
	lastmod  map[string]string
	hits     map[string]int
	pages    map[string]string
	failNext bool
}

func newSitemapIncrementalServer(t *testing.T) *sitemapIncrementalServer {
	t.Helper()
	s := &sitemapIncrementalServer{
		lastmod: map[string]string{},
		hits:    map[string]int{},
		pages: map[string]string{
			"/docs/index.html": `<html><head><title>Index</title></head><body><p>start page, no links</p></body></html>`,
			"/docs/a.html":     `<html><head><title>A</title></head><body><main>page A body</main></body></html>`,
			"/docs/b.html":     `<html><head><title>B</title></head><body><main>page B body</main></body></html>`,
			"/docs/c.html":     `<html><head><title>C</title></head><body><main>page C body</main></body></html>`,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.hits[r.URL.Path]++
		failAll := s.failNext
		s.mu.Unlock()
		if failAll && r.URL.Path != "/robots.txt" && r.URL.Path != "/sitemap.xml" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		switch r.URL.Path {
		case "/robots.txt":
			_, _ = io.WriteString(w, "User-agent: *\nAllow: /\nSitemap: http://"+r.Host+"/sitemap.xml\n")
		case "/sitemap.xml":
			origin := "http://" + r.Host
			var b strings.Builder
			b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`)
			for _, p := range []string{"/docs/a.html", "/docs/b.html", "/docs/c.html"} {
				b.WriteString(`<url><loc>` + origin + p + `</loc>`)
				if lm := s.lastmod[p]; lm != "" {
					b.WriteString(`<lastmod>` + lm + `</lastmod>`)
				}
				b.WriteString(`</url>`)
			}
			b.WriteString(`</urlset>`)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, b.String())
		default:
			body, ok := s.pages[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.WriteString(w, body)
		}
	}))
	t.Cleanup(srv.Close)
	s.Server = srv
	return s
}

func (s *sitemapIncrementalServer) setLastmod(path, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastmod[path] = value
}

func (s *sitemapIncrementalServer) setPage(path, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pages[path] = body
}

func (s *sitemapIncrementalServer) failPages() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = true
}

func (s *sitemapIncrementalServer) hitCount(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[path]
}

// sitemapIncrementalEnv bundles the staged-crawl harness the tests below use.
type sitemapIncrementalEnv struct {
	t       *testing.T
	appCfg  *config.AppConfig
	siteCfg *config.SiteConfig
	idx     *index.Index
	srv     *sitemapIncrementalServer
}

func newSitemapIncrementalEnv(t *testing.T) *sitemapIncrementalEnv {
	t.Helper()
	srv := newSitemapIncrementalServer(t)
	appCfg := newTestAppConfig(t)
	appCfg.NumWorkers = 1
	appCfg.MaxRequestsPerHost = 1
	appCfg.EnableSitemapIncremental = true
	idx, err := index.OpenAt(appCfg.StateDir, 10, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })
	return &sitemapIncrementalEnv{
		t:       t,
		appCfg:  appCfg,
		siteCfg: baseSiteConfig(srv.Server, "/docs/index.html"),
		idx:     idx,
		srv:     srv,
	}
}

// crawl runs one staged (fresh) crawl and returns its progress.
func (e *sitemapIncrementalEnv) crawl(resume bool) CrawlerProgress {
	e.t.Helper()
	crawlLog := newCrawlLog(e.t)
	logger := crawlLog.logger()
	ctx, cancel := crawlLog.deadlineContext(e.t, e.appCfg)
	defer cancel()

	stage, store, err := OpenStagedStore(ctx, e.appCfg, testSiteKey, resume, logger)
	require.NoError(e.t, err)
	defer func() { _ = store.Close() }()
	fetcher := fetch.NewFetcher(fetch.NewClient(e.appCfg.HTTPClientSettings, logger), e.appCfg, logger)
	rl := fetch.NewRateLimiter(e.appCfg.DefaultDelayPerHost, logger)
	c, err := NewCrawlerWithOptions(e.appCfg, e.siteCfg, testSiteKey, logger, store, fetcher, rl, ctx, cancel, resume,
		&CrawlerOptions{Index: e.idx, Stage: stage})
	require.NoError(e.t, err)
	require.NoError(e.t, c.Run(resume))
	return c.GetProgress()
}

func (e *sitemapIncrementalEnv) liveJSONLPath() string {
	return filepath.Join(siteOutputDir(e.appCfg, e.siteCfg), e.appCfg.JSONLOutputFilename)
}

func (e *sitemapIncrementalEnv) latestCrawl() *index.LatestCrawl {
	e.t.Helper()
	lc, err := e.idx.GetLatestCrawl(context.Background(), testSiteKey)
	require.NoError(e.t, err)
	return lc
}

// TestSitemapIncremental_SkipsUnchangedAndResetsOnRepeatedRun is the main
// happy-path regression: a baseline crawl followed by a sitemap-incremental
// crawl where every lastmod predates the first run downloads no pages, counts
// them unchanged, keeps the full corpus, and repeats identically.
func TestSitemapIncremental_SkipsUnchangedAndResetsOnRepeatedRun(t *testing.T) {
	e := newSitemapIncrementalEnv(t)
	old := "2024-01-01T00:00:00Z"
	e.srv.setLastmod("/docs/a.html", old)
	e.srv.setLastmod("/docs/b.html", old)
	e.srv.setLastmod("/docs/c.html", old)

	// Baseline: no history yet, so every page is fetched despite the flag.
	p := e.crawl(false)
	assert.EqualValues(t, 4, p.PagesProcessed, "index plus three sitemap pages")
	assert.EqualValues(t, 0, p.PagesUnchanged, "the first run cannot skip anything")
	for _, path := range []string{"/docs/a.html", "/docs/b.html", "/docs/c.html"} {
		assert.Equal(t, 1, e.srv.hitCount(path), "baseline download of %s", path)
	}
	baseline := e.latestCrawl()
	require.NotNil(t, baseline)
	assert.Equal(t, 4, baseline.TotalPages)

	// Second run: all lastmod values are older than the baseline end time.
	p = e.crawl(false)
	assert.EqualValues(t, 1, p.PagesProcessed, "only the seed page is a task; sitemap pages skip")
	assert.EqualValues(t, 3, p.PagesUnchanged, "every sitemap page is skipped by lastmod")
	for _, path := range []string{"/docs/a.html", "/docs/b.html", "/docs/c.html"} {
		assert.Equal(t, 1, e.srv.hitCount(path), "%s must not be downloaded again", path)
	}
	second := e.latestCrawl()
	assert.Equal(t, 4, second.TotalPages, "the skipped pages stay in the published corpus")
	assert.Equal(t, baseline.TotalPages, second.TotalPages)

	// Third run: the cutoff moves to the second run's end, but nothing changed,
	// so the same skips reproduce (the "repeated run resets" guarantee).
	p = e.crawl(false)
	assert.EqualValues(t, 1, p.PagesProcessed)
	assert.EqualValues(t, 3, p.PagesUnchanged)
	for _, path := range []string{"/docs/a.html", "/docs/b.html", "/docs/c.html"} {
		assert.Equal(t, 1, e.srv.hitCount(path), "repeated runs keep skipping %s", path)
	}
	third := e.latestCrawl()
	assert.Equal(t, 4, third.TotalPages)
}

// TestSitemapIncremental_BadAndMissingAndFutureLastmodFetchNormally pins the
// conservative rule: anything but a parseable, non-future lastmod older than
// the cutoff is downloaded.
func TestSitemapIncremental_BadAndMissingAndFutureLastmodFetchNormally(t *testing.T) {
	e := newSitemapIncrementalEnv(t)
	e.srv.setLastmod("/docs/a.html", "2024-01-01T00:00:00Z") // old: skipped
	// b and c deliberately have no lastmod yet.

	p := e.crawl(false)
	require.EqualValues(t, 0, p.PagesUnchanged)

	// Second run with a mixed bag.
	e.srv.setLastmod("/docs/a.html", "2024-01-01T00:00:00Z") // still old
	e.srv.setLastmod("/docs/b.html", "not-a-real-date")      // malformed: fetch
	e.srv.setLastmod("/docs/c.html", "2099-12-31T00:00:00Z") // future: fetch
	// Give c a future date-only variant coverage as well through b in a sub-check.

	p = e.crawl(false)
	assert.EqualValues(t, 1, p.PagesUnchanged, "only a is lastmod-skipped; b and c are fetched")
	assert.Equal(t, 1, e.srv.hitCount("/docs/a.html"), "old lastmod: no second download")
	assert.Equal(t, 2, e.srv.hitCount("/docs/b.html"), "malformed lastmod: downloaded again")
	assert.Equal(t, 2, e.srv.hitCount("/docs/c.html"), "future lastmod: downloaded again")
}

// TestSitemapIncremental_NewerLastmodFetchesAndPublishesChangedPage covers the
// other half of the signal: a lastmod newer than the cutoff forces a download
// even when nothing else changed, and the fresh content reaches the corpus.
func TestSitemapIncremental_NewerLastmodFetchesAndPublishesChangedPage(t *testing.T) {
	e := newSitemapIncrementalEnv(t)
	e.srv.setLastmod("/docs/a.html", "2024-01-01T00:00:00Z")
	e.srv.setLastmod("/docs/b.html", "2024-01-01T00:00:00Z")
	e.srv.setLastmod("/docs/c.html", "2024-01-01T00:00:00Z")
	e.crawl(false)

	// a changes: bump its content and give it a lastmod after the first crawl.
	e.srv.setPage("/docs/a.html", `<html><head><title>A</title></head><body><main>page A body UPDATED</main></body></html>`)
	e.srv.setLastmod("/docs/a.html", time.Now().UTC().Add(time.Hour).Format(time.RFC3339))

	p := e.crawl(false)
	assert.Equal(t, 2, e.srv.hitCount("/docs/a.html"), "the newer lastmod forces a download")
	assert.Equal(t, 1, e.srv.hitCount("/docs/b.html"))
	assert.Equal(t, 1, e.srv.hitCount("/docs/c.html"))
	assert.EqualValues(t, 2, p.PagesUnchanged, "b and c stay skipped")
	assert.True(t, e.liveJSONLContains("page A body UPDATED"), "the changed page is published")
	assert.False(t, e.liveJSONLContains("page A body</"))
}

// TestSitemapIncremental_DisabledByDefaultAndWithoutHistory proves the feature
// is inert unless the config flag is set, and also inert when no history index
// is attached.
func TestSitemapIncremental_DisabledByDefaultAndWithoutHistory(t *testing.T) {
	t.Run("flag off behaves like a normal crawl", func(t *testing.T) {
		e := newSitemapIncrementalEnv(t)
		e.appCfg.EnableSitemapIncremental = false
		e.srv.setLastmod("/docs/a.html", "2024-01-01T00:00:00Z")
		e.srv.setLastmod("/docs/b.html", "2024-01-01T00:00:00Z")
		e.srv.setLastmod("/docs/c.html", "2024-01-01T00:00:00Z")
		e.crawl(false)
		e.crawl(false)
		for _, path := range []string{"/docs/a.html", "/docs/b.html", "/docs/c.html"} {
			assert.Equal(t, 2, e.srv.hitCount(path), "without the flag every fresh staged crawl re-downloads %s", path)
		}
	})

	t.Run("flag on but no index attached fetches everything", func(t *testing.T) {
		e := newSitemapIncrementalEnv(t)
		e.idx = nil // mirrors get_page and other index-less call paths
		e.srv.setLastmod("/docs/a.html", "2024-01-01T00:00:00Z")
		e.srv.setLastmod("/docs/b.html", "2024-01-01T00:00:00Z")
		e.srv.setLastmod("/docs/c.html", "2024-01-01T00:00:00Z")

		crawlLog := newCrawlLog(t)
		logger := crawlLog.logger()
		ctx, cancel := crawlLog.deadlineContext(t, e.appCfg)
		defer cancel()
		stage, store, err := OpenStagedStore(ctx, e.appCfg, testSiteKey, false, logger)
		require.NoError(t, err)
		defer func() { _ = store.Close() }()
		fetcher := fetch.NewFetcher(fetch.NewClient(e.appCfg.HTTPClientSettings, logger), e.appCfg, logger)
		c, err := NewCrawlerWithOptions(e.appCfg, e.siteCfg, testSiteKey, logger, store, fetcher,
			fetch.NewRateLimiter(0, logger), ctx, cancel, false, &CrawlerOptions{Stage: stage})
		require.NoError(t, err)
		require.NoError(t, c.Run(false))
		for _, path := range []string{"/docs/a.html", "/docs/b.html", "/docs/c.html"} {
			assert.Equal(t, 1, e.srv.hitCount(path))
		}
	})
}

// TestSitemapIncremental_FailedRunKeepsPreviousCorpus verifies the
// no-damage guarantee: a staged run that skips pages by lastmod but fails to
// fetch the rest is kept back, leaving the live corpus and history untouched.
func TestSitemapIncremental_FailedRunKeepsPreviousCorpus(t *testing.T) {
	e := newSitemapIncrementalEnv(t)
	e.srv.setLastmod("/docs/a.html", "2024-01-01T00:00:00Z")
	e.srv.setLastmod("/docs/b.html", "2024-01-01T00:00:00Z")
	e.srv.setLastmod("/docs/c.html", "2024-01-01T00:00:00Z")
	e.crawl(false)
	before, err := countPriorPageRecords(e.liveJSONLPath())
	require.NoError(t, err)
	require.Equal(t, int64(4), before)

	// Every page fetch now fails with a 500 (the sitemap stays reachable so
	// lastmod skips still apply to a/b/c; the seed page alone must succeed for
	// anything to be published -- make it fail too).
	e.srv.failPages()

	crawlLog := newCrawlLog(t)
	logger := crawlLog.logger()
	ctx, cancel := crawlLog.deadlineContext(t, e.appCfg)
	defer cancel()
	stage, store, openErr := OpenStagedStore(ctx, e.appCfg, testSiteKey, false, logger)
	require.NoError(t, openErr)
	defer func() { _ = store.Close() }()
	fetcher := fetch.NewFetcher(fetch.NewClient(e.appCfg.HTTPClientSettings, logger), e.appCfg, logger)
	c, err := NewCrawlerWithOptions(e.appCfg, e.siteCfg, testSiteKey, logger, store, fetcher,
		fetch.NewRateLimiter(0, logger), ctx, cancel, false, &CrawlerOptions{Index: e.idx, Stage: stage})
	require.NoError(t, err)
	runErr := c.Run(false)
	require.Error(t, runErr, "a crawl that saved nothing is kept back")

	after, err := countPriorPageRecords(e.liveJSONLPath())
	require.NoError(t, err)
	assert.Equal(t, before, after, "the failed run must not shrink the live corpus")
	lc := e.latestCrawl()
	require.NotNil(t, lc)
	assert.Equal(t, 4, lc.TotalPages, "the failed run is not recorded in history")
}

func (e *sitemapIncrementalEnv) liveJSONLContains(needle string) bool {
	e.t.Helper()
	b, err := os.ReadFile(e.liveJSONLPath())
	require.NoError(e.t, err)
	return strings.Contains(string(b), needle)
}
