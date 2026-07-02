package naming

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

// testResolver creates a fresh resolver with no preloaded entries.
func testResolver(t *testing.T) *HostsTxtResolver {
	t.Helper()
	r := &HostsTxtResolver{
		hosts: make(map[string][]byte),
	}
	return r
}

// hostsLine returns a valid hosts.txt line. "AAAA" is a valid I2P base64
// encoding per the existing parseHostsLine tests in resolver_test.go.
func hostsLine(hostname string) string {
	return hostname + "=AAAA\n"
}

// testHostsServer returns an httptest server serving a given hosts.txt body.
func testHostsServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// =============================================================================
// Add / Remove / List
// =============================================================================

func TestSubscriptionManager_AddRemoveList(t *testing.T) {
	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	assert.Len(t, sm.List(), 0, "new manager should have no subscriptions")

	sm.Add("http://example.com/a.txt", "alpha", 0)
	assert.Len(t, sm.List(), 1)

	sm.Add("http://example.com/b.txt", "beta", 30*time.Minute)
	assert.Len(t, sm.List(), 2)

	// Duplicate URL replaces
	sm.Add("http://example.com/a.txt", "alpha-v2", 1*time.Hour)
	list := sm.List()
	assert.Len(t, list, 2)
	for _, s := range list {
		if s.URL == "http://example.com/a.txt" {
			assert.Equal(t, "alpha-v2", s.Name)
			assert.Equal(t, 1*time.Hour, s.Interval)
		}
	}

	sm.Remove("http://example.com/a.txt")
	assert.Len(t, sm.List(), 1)

	sm.Remove("http://example.com/nonexistent.txt") // no-op
	assert.Len(t, sm.List(), 1)
}

func TestSubscriptionManager_DefaultInterval(t *testing.T) {
	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	sm.Add("http://example.com/hosts.txt", "test", 0)
	list := sm.List()
	require.Len(t, list, 1)
	assert.Equal(t, defaultSubscriptionInterval, list[0].Interval)
}

func TestSubscriptionManager_ListReturnsCopy(t *testing.T) {
	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	sm.Add("http://example.com/a.txt", "a", time.Hour)

	list1 := sm.List()
	list1[0].Name = "modified"

	list2 := sm.List()
	assert.Equal(t, "a", list2[0].Name, "list should return a copy, not a reference")
}

// =============================================================================
// FetchAll
// =============================================================================

func TestFetchAll_Success(t *testing.T) {
	srv := testHostsServer(t, hostsLine("test.i2p"))
	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	sm.Add(srv.URL, "test-sub", time.Hour)

	err := sm.FetchAll()
	require.NoError(t, err)

	dest, err := r.ResolveHostname("test.i2p")
	require.NoError(t, err)
	assert.NotEmpty(t, dest)
}

func TestFetchAll_MultipleSources(t *testing.T) {
	srv1 := testHostsServer(t, hostsLine("alpha.i2p"))
	srv2 := testHostsServer(t, hostsLine("beta.i2p"))

	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	sm.Add(srv1.URL, "alpha", time.Hour)
	sm.Add(srv2.URL, "beta", time.Hour)

	err := sm.FetchAll()
	require.NoError(t, err)

	_, err = r.ResolveHostname("alpha.i2p")
	assert.NoError(t, err)
	_, err = r.ResolveHostname("beta.i2p")
	assert.NoError(t, err)
}

func TestFetchAll_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	r := testResolver(t)
	sm := NewSubscriptionManager(r)
	sm.Add(srv.URL, "broken", time.Hour)

	err := sm.FetchAll()
	assert.Error(t, err)
}

func TestFetchAll_MalformedLines(t *testing.T) {
	body := "# this is a comment\n\n" +
		hostsLine("valid.i2p") +
		"line_without_equals\n" +
		"also_bad=!!!invalid_base64!!!\n"

	srv := testHostsServer(t, body)

	r := testResolver(t)
	sm := NewSubscriptionManager(r)
	sm.Add(srv.URL, "mixed", time.Hour)

	err := sm.FetchAll()
	// Malformed lines are skipped, not fatal
	require.NoError(t, err)

	_, err = r.ResolveHostname("valid.i2p")
	assert.NoError(t, err)

	_, err = r.ResolveHostname("also_bad.i2p")
	assert.Error(t, err)
}

func TestFetchAll_ResponseTooLarge(t *testing.T) {
	srv := testHostsServer(t, strings.Repeat("a", maxSubscriptionBody+1))
	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	sm.Add(srv.URL, "too-large", time.Hour)

	err := sm.FetchAll()
	assert.Error(t, err)
}

// =============================================================================
// Periodic refresh
// =============================================================================

func TestPeriodicRefresh(t *testing.T) {
	srv := testHostsServer(t, hostsLine("refresh.i2p"))
	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	sm.Add(srv.URL, "refresh-test", 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sm.Start(ctx)

	assert.Eventually(t, func() bool {
		_, err := r.ResolveHostname("refresh.i2p")
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "first fetch should populate entry")

	sm.Stop()
}

func TestStopCancelsFetch(t *testing.T) {
	requestStarted := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	r := testResolver(t)
	sm := NewSubscriptionManager(r)
	sm.Add(srv.URL, "blocking", 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sm.Start(ctx)
	<-requestStarted

	// Stop should cancel the in-flight request and return promptly.
	start := time.Now()
	sm.Stop()
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestStartAfterStopRestarts(t *testing.T) {
	var mu sync.Mutex
	hostname := "first.i2p"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		body := hostsLine(hostname)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	r := testResolver(t)
	sm := NewSubscriptionManager(r)
	sm.Add(srv.URL, "restart-test", 50*time.Millisecond)

	sm.Start(context.Background())
	assert.Eventually(t, func() bool {
		_, err := r.ResolveHostname("first.i2p")
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)
	sm.Stop()

	mu.Lock()
	hostname = "second.i2p"
	mu.Unlock()

	sm.Start(context.Background())
	assert.Eventually(t, func() bool {
		_, err := r.ResolveHostname("second.i2p")
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)
	sm.Stop()
}

// =============================================================================
// Concurrent safety
// =============================================================================

func TestConcurrentAddAndFetch(t *testing.T) {
	srv := testHostsServer(t, hostsLine("concurrent.i2p"))
	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sm.Add(srv.URL, "concurrent", time.Hour)
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sm.FetchAll()
		}()
	}

	wg.Wait()

	dest, err := r.ResolveHostname("concurrent.i2p")
	assert.NoError(t, err)
	assert.NotEmpty(t, dest)
}

func TestNilResolverPanics(t *testing.T) {
	assert.Panics(t, func() {
		NewSubscriptionManager(nil)
	})
}

// =============================================================================
// Idempotent Start
// =============================================================================

func TestStartIsIdempotent(t *testing.T) {
	r := testResolver(t)
	sm := NewSubscriptionManager(r)

	srv := testHostsServer(t, hostsLine("idem.i2p"))
	sm.Add(srv.URL, "idem-test", 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sm.Start(ctx)
	sm.Start(ctx) // second call is no-op

	assert.Eventually(t, func() bool {
		_, err := r.ResolveHostname("idem.i2p")
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)

	sm.Stop()
}
