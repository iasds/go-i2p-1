package naming

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/go-i2p/logger"
	"github.com/samber/oops"
)

// ──────────────────────────────────────────────────────────────────────────────
// Subscription types
// ──────────────────────────────────────────────────────────────────────────────

// defaultSubscriptionInterval is the default fetch interval for subscriptions
// that do not specify one. Matches i2pd's default of 12 hours.
const defaultSubscriptionInterval = 12 * time.Hour

// maxSubscriptionBody is the maximum size of a remote hosts.txt response.
const maxSubscriptionBody = 1 << 20 // 1 MiB

// Subscription represents a remote hosts.txt source that is fetched
// periodically and merged into the resolver.
type Subscription struct {
	URL      string
	Name     string
	Interval time.Duration
}

// SubscriptionManager periodically fetches remote hosts.txt files and
// merges entries into a HostsTxtResolver.
//
// Each subscription runs on an independent goroutine with its own
// ticker. HTTP errors are logged but never fatal — the manager
// continues fetching other sources and retries on the next tick.
type SubscriptionManager struct {
	mu            sync.Mutex
	subscriptions map[string]*Subscription // keyed by URL
	resolver      *HostsTxtResolver
	client        *http.Client
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// NewSubscriptionManager creates a SubscriptionManager backed by
// the given resolver. The resolver MUST NOT be nil.
func NewSubscriptionManager(resolver *HostsTxtResolver) *SubscriptionManager {
	if resolver == nil {
		panic("SubscriptionManager requires a non-nil HostsTxtResolver")
	}

	return &SubscriptionManager{
		subscriptions: make(map[string]*Subscription),
		resolver:      resolver,
		client: &http.Client{
			Timeout: DefaultFetchTimeout,
		},
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Subscription management
// ──────────────────────────────────────────────────────────────────────────────

// Add registers a subscription. If a subscription with the same URL already
// exists, it is replaced (interval and name are updated).
//
// If interval is zero, the default (12h) is used.
func (sm *SubscriptionManager) Add(url, name string, interval time.Duration) {
	if interval <= 0 {
		interval = defaultSubscriptionInterval
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.subscriptions[url] = &Subscription{
		URL:      url,
		Name:     name,
		Interval: interval,
	}

	log.WithFields(logger.Fields{
		"at":       "(SubscriptionManager) Add",
		"url":      url,
		"name":     name,
		"interval": interval.String(),
		"reason":   "subscription registered",
	}).Info("subscription_added")
}

// Remove deletes a subscription by URL. No-op if the URL is not registered.
func (sm *SubscriptionManager) Remove(url string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.subscriptions[url]; ok {
		delete(sm.subscriptions, url)

		log.WithFields(logger.Fields{
			"at":     "(SubscriptionManager) Remove",
			"url":    url,
			"reason": "subscription removed",
		}).Info("subscription_removed")
	}
}

// List returns a snapshot of all currently registered subscriptions.
// The returned slice is safe to read; modifications to the manager
// do not affect it.
func (sm *SubscriptionManager) List() []Subscription {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	result := make([]Subscription, 0, len(sm.subscriptions))
	for _, s := range sm.subscriptions {
		result = append(result, *s)
	}
	return result
}

// ──────────────────────────────────────────────────────────────────────────────
// Fetching
// ──────────────────────────────────────────────────────────────────────────────

// FetchAll fetches every registered subscription immediately.
//
// If one or more sources fail, the first error is returned but
// FetchAll continues to fetch remaining sources. Individual fetch
// failures are logged at warn level.
func (sm *SubscriptionManager) FetchAll() error {
	sm.mu.Lock()
	subs := make([]*Subscription, 0, len(sm.subscriptions))
	for _, s := range sm.subscriptions {
		subs = append(subs, s)
	}
	sm.mu.Unlock()

	var firstErr error

	for _, s := range subs {
		if err := sm.fetchOne(s); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// fetchOne fetches a single subscription and merges its entries.
func (sm *SubscriptionManager) fetchOne(s *Subscription) error {
	log.WithFields(logger.Fields{
		"at":   "(SubscriptionManager) fetchOne",
		"url":  s.URL,
		"name": s.Name,
	}).Debug("fetching_subscription")

	ctx := sm.currentContext()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return oops.Wrapf(err, "failed to create request for %s (%s)", s.Name, s.URL)
	}

	resp, err := sm.client.Do(req)
	if err != nil {
		log.WithFields(logger.Fields{
			"at":     "(SubscriptionManager) fetchOne",
			"url":    s.URL,
			"name":   s.Name,
			"error":  err.Error(),
			"reason": "http_request_failed",
		}).Warn("subscription_fetch_failed")
		return oops.Wrapf(err, "failed to fetch %s (%s)", s.Name, s.URL)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.WithFields(logger.Fields{
			"at":     "(SubscriptionManager) fetchOne",
			"url":    s.URL,
			"name":   s.Name,
			"status": resp.StatusCode,
			"reason": "non-2xx response",
		}).Warn("subscription_fetch_non_2xx")
		return oops.Errorf("non-2xx status %d from %s (%s)", resp.StatusCode, s.Name, s.URL)
	}

	// Limit response body to prevent memory exhaustion
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSubscriptionBody+1))
	if err != nil {
		log.WithFields(logger.Fields{
			"at":     "(SubscriptionManager) fetchOne",
			"url":    s.URL,
			"name":   s.Name,
			"error":  err.Error(),
			"reason": "read_body_failed",
		}).Warn("subscription_read_failed")
		return oops.Wrapf(err, "failed to read body from %s (%s)", s.Name, s.URL)
	}
	if len(body) > maxSubscriptionBody {
		log.WithFields(logger.Fields{
			"at":     "(SubscriptionManager) fetchOne",
			"url":    s.URL,
			"name":   s.Name,
			"limit":  maxSubscriptionBody,
			"reason": "response_too_large",
		}).Warn("subscription_response_too_large")
		return oops.Errorf("response from %s (%s) exceeds %d bytes", s.Name, s.URL, maxSubscriptionBody)
	}

	// Merge entries into the resolver. addHostsData handles parsing,
	// dedup, and logging — we just pass the raw bytes and source label.
	source := fmt.Sprintf("subscription:%s (%s)", s.Name, s.URL)
	if err := sm.resolver.addHostsData(body, source); err != nil {
		log.WithFields(logger.Fields{
			"at":     "(SubscriptionManager) fetchOne",
			"url":    s.URL,
			"name":   s.Name,
			"error":  err.Error(),
			"reason": "merge_failed",
		}).Warn("subscription_merge_failed")
		return oops.Wrapf(err, "failed to merge entries from %s (%s)", s.Name, s.URL)
	}

	log.WithFields(logger.Fields{
		"at":   "(SubscriptionManager) fetchOne",
		"url":  s.URL,
		"name": s.Name,
	}).Debug("subscription_fetch_complete")

	return nil
}

// currentContext returns the manager context when running, or Background for
// one-off fetches before Start is called.
func (sm *SubscriptionManager) currentContext() context.Context {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.ctx != nil {
		return sm.ctx
	}
	return context.Background()
}

// ──────────────────────────────────────────────────────────────────────────────
// Lifecycle
// ──────────────────────────────────────────────────────────────────────────────

// Start begins periodic fetching of all registered subscriptions.
//
// Each subscription runs in its own goroutine. On start, each subscription
// is fetched immediately (not waiting for the first tick). A subscription
// added after Start() is called will NOT be picked up by the periodic loop
// until the manager is stopped and started again. Use FetchAll() for one-off
// fetches.
//
// Start is idempotent — calling it on an already-started manager is a no-op.
func (sm *SubscriptionManager) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	sm.mu.Lock()
	// Idempotency check: if we already have a context, we're running.
	if sm.ctx != nil {
		sm.mu.Unlock()
		log.WithFields(logger.Fields{
			"at": "(SubscriptionManager) Start",
		}).Debug("subscription_manager_already_started")
		return
	}

	sm.ctx, sm.cancel = context.WithCancel(ctx)

	// Snapshot subscriptions so we don't hold the lock during goroutine launch.
	subs := make([]*Subscription, 0, len(sm.subscriptions))
	for _, s := range sm.subscriptions {
		subs = append(subs, s)
	}
	sm.mu.Unlock()

	for _, s := range subs {
		sm.wg.Add(1)
		go sm.runSubscription(s)
	}

	log.WithFields(logger.Fields{
		"at":    "(SubscriptionManager) Start",
		"count": len(subs),
	}).Info("subscription_manager_started")
}

// Stop cancels periodic fetching and waits for in-flight fetches to finish.
//
// After Stop returns, the manager can be restarted with a new context
// via Start().
func (sm *SubscriptionManager) Stop() {
	sm.mu.Lock()
	if sm.cancel != nil {
		sm.cancel()
	}
	sm.mu.Unlock()

	sm.wg.Wait()

	sm.mu.Lock()
	sm.ctx = nil
	sm.cancel = nil
	sm.mu.Unlock()

	log.WithFields(logger.Fields{
		"at": "(SubscriptionManager) Stop",
	}).Info("subscription_manager_stopped")
}

// runSubscription is the per-subscription goroutine. It fetches immediately
// on start and then on each ticker interval. Exits when the manager's context
// is cancelled.
func (sm *SubscriptionManager) runSubscription(s *Subscription) {
	defer sm.wg.Done()

	// Fetch immediately on start
	if err := sm.fetchOne(s); err != nil {
		// Already logged in fetchOne
		_ = err
	}

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			log.WithFields(logger.Fields{
				"at":   "(SubscriptionManager) runSubscription",
				"url":  s.URL,
				"name": s.Name,
			}).Debug("subscription_goroutine_stopping")
			return
		case <-ticker.C:
			if err := sm.fetchOne(s); err != nil {
				// Already logged in fetchOne
				_ = err
			}
		}
	}
}
