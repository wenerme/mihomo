package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	"github.com/metacubex/mihomo/common/lru"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/common/xsync"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"golang.org/x/net/publicsuffix"
)

type strategyFn = func(proxies []C.Proxy, metadata *C.Metadata, touch bool) C.Proxy

type LoadBalance struct {
	*GroupBase
	disableUDP     bool
	strategyFn     strategyFn
	testUrl        string
	expectedStatus string
}

var errStrategy = errors.New("unsupported strategy")

// Global adaptive state for all proxies (shared across all load-balance groups)
var (
	globalAdaptiveStates xsync.Map[string, *adaptiveState]
	defaultAdaptiveAlpha = 0.3
)

func parseStrategy(config map[string]any) string {
	if strategy, ok := config["strategy"].(string); ok {
		return strategy
	}
	return "consistent-hashing"
}

func getKey(metadata *C.Metadata) string {
	if metadata == nil {
		return ""
	}

	if metadata.Host != "" {
		// ip host
		if ip := net.ParseIP(metadata.Host); ip != nil {
			return metadata.Host
		}

		if etld, err := publicsuffix.EffectiveTLDPlusOne(metadata.Host); err == nil {
			return etld
		}
	}

	if !metadata.DstIP.IsValid() {
		return ""
	}

	return metadata.DstIP.String()
}

func getKeyWithSrcAndDst(metadata *C.Metadata) string {
	dst := getKey(metadata)
	src := ""
	if metadata != nil {
		src = metadata.SrcIP.String()
	}

	return fmt.Sprintf("%s%s", src, dst)
}

func jumpHash(key uint64, buckets int32) int32 {
	var b, j int64

	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

// DialContext implements C.ProxyAdapter
func (lb *LoadBalance) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	proxy := lb.Unwrap(metadata, true)
	proxyName := proxy.Name()
	start := time.Now()

	c, err = proxy.DialContext(ctx, metadata)

	if err == nil {
		c.AppendToChains(lb)
		// Record successful dial latency for adaptive strategy
		RecordProxySuccess(proxyName, time.Since(start))
	} else {
		lb.onDialFailed(proxy.Type(), err, lb.healthCheck)
		// Record failure for adaptive strategy
		RecordProxyFailure(proxyName)
	}

	if N.NeedHandshake(c) {
		handshakeStart := time.Now()
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				lb.onDialSuccess()
				// Record successful handshake latency
				RecordProxySuccess(proxyName, time.Since(handshakeStart))
			} else {
				lb.onDialFailed(proxy.Type(), err, lb.healthCheck)
				// Record handshake failure
				RecordProxyFailure(proxyName)
			}
		})
	}

	return
}

// ListenPacketContext implements C.ProxyAdapter
func (lb *LoadBalance) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (pc C.PacketConn, err error) {
	defer func() {
		if err == nil {
			pc.AppendToChains(lb)
		}
	}()

	proxy := lb.Unwrap(metadata, true)
	return proxy.ListenPacketContext(ctx, metadata)
}

// SupportUDP implements C.ProxyAdapter
func (lb *LoadBalance) SupportUDP() bool {
	return !lb.disableUDP
}

// IsL3Protocol implements C.ProxyAdapter
func (lb *LoadBalance) IsL3Protocol(metadata *C.Metadata) bool {
	return lb.Unwrap(metadata, false).IsL3Protocol(metadata)
}

func strategyRoundRobin(url string) strategyFn {
	idx := 0
	idxMutex := sync.Mutex{}
	return func(proxies []C.Proxy, metadata *C.Metadata, touch bool) C.Proxy {
		idxMutex.Lock()
		defer idxMutex.Unlock()

		i := 0
		length := len(proxies)

		if touch {
			defer func() {
				idx = (idx + i) % length
			}()
		}

		for ; i < length; i++ {
			id := (idx + i) % length
			proxy := proxies[id]
			if proxy.AliveForTestUrl(url) {
				i++
				return proxy
			}
		}

		return proxies[0]
	}
}

func strategyConsistentHashing(url string) strategyFn {
	maxRetry := 5
	return func(proxies []C.Proxy, metadata *C.Metadata, touch bool) C.Proxy {
		key := utils.MapHash(getKey(metadata))
		buckets := int32(len(proxies))
		for i := 0; i < maxRetry; i, key = i+1, key+1 {
			idx := jumpHash(key, buckets)
			proxy := proxies[idx]
			if proxy.AliveForTestUrl(url) {
				return proxy
			}
		}

		// when availability is poor, traverse the entire list to get the available nodes
		for _, proxy := range proxies {
			if proxy.AliveForTestUrl(url) {
				return proxy
			}
		}

		return proxies[0]
	}
}

func strategyStickySessions(url string) strategyFn {
	ttl := time.Minute * 10
	maxRetry := 5
	lruCache := lru.New[uint64, int](
		lru.WithAge[uint64, int](int64(ttl.Seconds())),
		lru.WithSize[uint64, int](1000))
	return func(proxies []C.Proxy, metadata *C.Metadata, touch bool) C.Proxy {
		key := utils.MapHash(getKeyWithSrcAndDst(metadata))
		length := len(proxies)
		idx, has := lruCache.Get(key)
		if !has || idx >= length {
			idx = int(jumpHash(key+uint64(time.Now().UnixNano()), int32(length)))
		}

		nowIdx := idx
		for i := 1; i < maxRetry; i++ {
			proxy := proxies[nowIdx]
			if proxy.AliveForTestUrl(url) {
				if !has || nowIdx != idx {
					lruCache.Set(key, nowIdx)
				}

				return proxy
			} else {
				nowIdx = int(jumpHash(key+uint64(time.Now().UnixNano()), int32(length)))
			}
		}

		lruCache.Set(key, 0)
		return proxies[0]
	}
}

// adaptiveState holds EWMA metrics for a single proxy based on real connection metrics
type adaptiveState struct {
	ewmaLatency     float64 // EWMA connection latency in ms
	ewmaFailureRate float64 // EWMA failure rate [0, 1]
	ewmaJitter      float64 // EWMA jitter (latency variance)
	lastLatency     float64 // last observed latency for jitter calculation
	sampleCount     int     // number of samples collected
	mu              sync.RWMutex
}

// AdaptiveOptions configures the adaptive load balance strategy
type AdaptiveOptions struct {
	Alpha         float64 // EWMA decay factor (0.1-0.5), default 0.3
	LatencyWeight float64 // weight for latency score, default 0.5
	FailureWeight float64 // weight for failure rate score, default 0.3
	JitterWeight  float64 // weight for jitter score, default 0.2
	MinSamples    int     // minimum samples before adaptive kicks in, default 3
}

// RecordProxySuccess records a successful connection with latency to global adaptive state
func RecordProxySuccess(proxyName string, latency time.Duration) {
	state, _ := globalAdaptiveStates.LoadOrStoreFn(proxyName, func() *adaptiveState {
		return &adaptiveState{}
	})

	state.mu.Lock()
	defer state.mu.Unlock()

	latencyMs := float64(latency.Milliseconds())
	alpha := defaultAdaptiveAlpha

	// Update failure rate (success = 0)
	state.ewmaFailureRate = alpha*0.0 + (1-alpha)*state.ewmaFailureRate

	// Update latency EWMA
	if state.sampleCount == 0 {
		state.ewmaLatency = latencyMs
	} else {
		state.ewmaLatency = alpha*latencyMs + (1-alpha)*state.ewmaLatency
	}

	// Update jitter EWMA
	if state.sampleCount > 0 {
		jitter := math.Abs(latencyMs - state.lastLatency)
		state.ewmaJitter = alpha*jitter + (1-alpha)*state.ewmaJitter
	}

	state.lastLatency = latencyMs
	state.sampleCount++
}

// RecordProxyFailure records a failed connection to global adaptive state
func RecordProxyFailure(proxyName string) {
	state, _ := globalAdaptiveStates.LoadOrStoreFn(proxyName, func() *adaptiveState {
		return &adaptiveState{}
	})

	state.mu.Lock()
	defer state.mu.Unlock()

	alpha := defaultAdaptiveAlpha
	// Update failure rate (failure = 1)
	state.ewmaFailureRate = alpha*1.0 + (1-alpha)*state.ewmaFailureRate
	state.sampleCount++
}

// GetProxyAdaptiveState returns the adaptive metrics for a proxy (for debugging/monitoring)
func GetProxyAdaptiveState(proxyName string) (latency, failureRate, jitter float64, samples int) {
	state, ok := globalAdaptiveStates.Load(proxyName)
	if !ok {
		return 0, 0, 0, 0
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.ewmaLatency, state.ewmaFailureRate, state.ewmaJitter, state.sampleCount
}

func parseAdaptiveOptions(config map[string]any) *AdaptiveOptions {
	opts := &AdaptiveOptions{
		Alpha:         0.3,
		LatencyWeight: 0.5,
		FailureWeight: 0.3,
		JitterWeight:  0.2,
		MinSamples:    3,
	}
	if alpha, ok := config["alpha"].(float64); ok {
		opts.Alpha = math.Max(0.1, math.Min(0.5, alpha))
	}
	if w, ok := config["latency-weight"].(float64); ok {
		opts.LatencyWeight = w
	}
	if w, ok := config["failure-weight"].(float64); ok {
		opts.FailureWeight = w
	}
	if w, ok := config["jitter-weight"].(float64); ok {
		opts.JitterWeight = w
	}
	if ms, ok := config["min-samples"].(int); ok && ms > 0 {
		opts.MinSamples = ms
	}
	return opts
}

func strategyAdaptive(url string, opts *AdaptiveOptions) strategyFn {
	return func(proxies []C.Proxy, metadata *C.Metadata, touch bool) C.Proxy {
		// 1. Filter alive proxies (still use URL test for liveness check)
		var aliveProxies []C.Proxy
		for _, p := range proxies {
			if p.AliveForTestUrl(url) {
				aliveProxies = append(aliveProxies, p)
			}
		}
		if len(aliveProxies) == 0 {
			return proxies[0] // fallback to first proxy
		}

		// 2. Calculate weights based on global real connection metrics
		weights := make([]float64, len(aliveProxies))
		for i, p := range aliveProxies {
			state, _ := globalAdaptiveStates.LoadOrStoreFn(p.Name(), func() *adaptiveState {
				return &adaptiveState{}
			})
			weights[i] = calculateAdaptiveWeight(state, opts)
		}

		// 3. Weighted random selection
		return weightedRandomSelect(aliveProxies, weights)
	}
}

func calculateAdaptiveWeight(state *adaptiveState, opts *AdaptiveOptions) float64 {
	state.mu.RLock()
	defer state.mu.RUnlock()

	// Not enough samples, return default weight for uniform distribution
	if state.sampleCount < opts.MinSamples {
		return 1.0
	}

	// Calculate scores (higher is better)
	// Latency score: lower latency -> higher score
	latencyScore := 100.0 / math.Max(state.ewmaLatency, 1.0)

	// Success score: lower failure rate -> higher score
	successScore := 1.0 - state.ewmaFailureRate

	// Stability score: lower jitter -> higher score
	stabilityScore := 100.0 / math.Max(state.ewmaJitter, 1.0)

	// Weighted sum
	weight := opts.LatencyWeight*latencyScore +
		opts.FailureWeight*successScore +
		opts.JitterWeight*stabilityScore

	return math.Max(weight, 0.01) // Minimum weight to avoid zero probability
}

func weightedRandomSelect(proxies []C.Proxy, weights []float64) C.Proxy {
	total := 0.0
	for _, w := range weights {
		total += w
	}

	r := rand.Float64() * total
	cumulative := 0.0
	for i, w := range weights {
		cumulative += w
		if r <= cumulative {
			return proxies[i]
		}
	}
	return proxies[len(proxies)-1]
}

// Unwrap implements C.ProxyAdapter
func (lb *LoadBalance) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxies := lb.GetProxies(touch)
	return lb.strategyFn(proxies, metadata, touch)
}

// MarshalJSON implements C.ProxyAdapter
func (lb *LoadBalance) MarshalJSON() ([]byte, error) {
	var all []string
	for _, proxy := range lb.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	return json.Marshal(map[string]any{
		"type":           lb.Type().String(),
		"all":            all,
		"testUrl":        lb.testUrl,
		"expectedStatus": lb.expectedStatus,
		"hidden":         lb.Hidden(),
		"icon":           lb.Icon(),
	})
}

func (lb *LoadBalance) Providers() []P.ProxyProvider {
	return lb.providers
}

func (lb *LoadBalance) Proxies() []C.Proxy {
	return lb.GetProxies(false)
}

func (lb *LoadBalance) Now() string {
	return ""
}

func NewLoadBalance(option *GroupCommonOption, providers []P.ProxyProvider, strategy string, config map[string]any) (lb *LoadBalance, err error) {
	var strategyFn strategyFn
	switch strategy {
	case "consistent-hashing":
		strategyFn = strategyConsistentHashing(option.URL)
	case "round-robin":
		strategyFn = strategyRoundRobin(option.URL)
	case "sticky-sessions":
		strategyFn = strategyStickySessions(option.URL)
	case "adaptive":
		opts := parseAdaptiveOptions(config)
		strategyFn = strategyAdaptive(option.URL, opts)
	default:
		return nil, fmt.Errorf("%w: %s", errStrategy, strategy)
	}
	return &LoadBalance{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.LoadBalance,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			Providers:      providers,
		}),
		strategyFn:     strategyFn,
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
	}, nil
}
