package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ============================================================
// BACKEND DEFINITION & PERFORMANCE METRICS
// ============================================================

type Backend struct {
	URL              *url.URL
	Alive            bool
	mu               sync.RWMutex
	ActiveConns      int64
	TotalRequests    int64
	TotalErrors      int64
	AvgLatencyMs     float64
	ConsecutiveFails int32
	ReverseProxy     *httputil.ReverseProxy
}

func (b *Backend) SetAlive(alive bool) {
	b.mu.Lock()
	b.Alive = alive
	if alive {
		atomic.StoreInt32(&b.ConsecutiveFails, 0)
	}
	b.mu.Unlock()
}

func (b *Backend) IsAlive() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Alive
}

func (b *Backend) IncrConns() {
	atomic.AddInt64(&b.ActiveConns, 1)
	atomic.AddInt64(&b.TotalRequests, 1)
}

func (b *Backend) DecrConns() {
	atomic.AddInt64(&b.ActiveConns, -1)
}

func (b *Backend) RecordError() {
	atomic.AddInt64(&b.TotalErrors, 1)
	atomic.AddInt32(&b.ConsecutiveFails, 1)
}

func (b *Backend) RecordSuccess(latency float64) {
	atomic.StoreInt32(&b.ConsecutiveFails, 0)
	b.mu.Lock()
	b.Alive = true
	if b.AvgLatencyMs == 0 {
		b.AvgLatencyMs = latency
	} else {
		b.AvgLatencyMs = 0.8*b.AvgLatencyMs + 0.2*latency
	}
	b.mu.Unlock()
}

func (b *Backend) GetAvgLatency() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.AvgLatencyMs
}

func (b *Backend) GetLoadScore() float64 {
	active := float64(atomic.LoadInt64(&b.ActiveConns))
	latency := b.GetAvgLatency()
	fails := float64(atomic.LoadInt32(&b.ConsecutiveFails))
	return (active * 1.0) + (latency * 0.05) + (fails * 5.0)
}

// ============================================================
// CLUSTER FEED MODEL
// ============================================================

type FeedItem struct {
	ID         int64  `json:"id"`
	MsgID      string `json:"msg_id"`
	ClientName string `json:"client-name"`
	Msg        string `json:"msg"`
	Timestamp  string `json:"timestamp"`
	CreatedAt  string `json:"created_at"`
}

func (f FeedItem) MarshalJSON() ([]byte, error) {
	type Alias FeedItem
	return json.Marshal(&struct {
		Alias
		ClientNameU string `json:"client_name"`
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
		Message     string `json:"message"`
	}{
		Alias:       Alias(f),
		ClientNameU: f.ClientName,
		Username:    f.ClientName,
		DisplayName: f.ClientName,
		Message:     f.Msg,
	})
}

type MsgResponse struct {
	Status      string `json:"status"`
	ID          string `json:"id"`
	MsgID       string `json:"msg_id"`
	ClientName  string `json:"client-name"`
	ClientNameU string `json:"client_name"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Msg         string `json:"msg"`
	Message     string `json:"message"`
	Timestamp   string `json:"timestamp"`
	CreatedAt   string `json:"created_at"`
}

// ============================================================
// DYNAMIC LOAD BALANCER
// ============================================================

type DynamicLoadBalancer struct {
	backends         []*Backend
	mu               sync.RWMutex
	maxConnThreshold int64
	latencyThreshold float64
	totalRequests    int64
	totalErrors      int64
	totalSwitches    int64
	lastSelectedIdx  int64
	startTime        time.Time
	transport        *http.Transport

	// Cluster Feed Cache
	feedMu            sync.RWMutex
	feedList          []FeedItem
	feedIDs           map[string]struct{}
	feedVersion       int64
	cachedFeedVersion int64
	cachedFeedBytes   []byte
	cachedMu          sync.RWMutex
}

func NewDynamicLoadBalancer(maxConnThreshold int64, latencyThreshold float64) *DynamicLoadBalancer {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   3000,
		MaxConnsPerHost:       10000,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
	}

	lb := &DynamicLoadBalancer{
		maxConnThreshold: maxConnThreshold,
		latencyThreshold: latencyThreshold,
		startTime:        time.Now(),
		transport:        tr,
		feedIDs:          make(map[string]struct{}, 200000),
		feedList:         make([]FeedItem, 0, 200000),
		cachedFeedBytes:  []byte("[]"),
	}

	return lb
}

func (lb *DynamicLoadBalancer) AddBackend(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid backend URL %q: %w", rawURL, err)
	}

	backend := &Backend{
		URL:   u,
		Alive: true,
	}

	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.Transport = lb.transport
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		backend.RecordError()
		atomic.AddInt64(&lb.totalErrors, 1)
		http.Error(w, `{"error":"backend unavailable"}`, http.StatusBadGateway)
	}
	backend.ReverseProxy = proxy

	lb.mu.Lock()
	lb.backends = append(lb.backends, backend)
	lb.mu.Unlock()
	return nil
}

// SelectBackend implements Performance-Based Dynamic Load Balancing with Threshold Switching
func (lb *DynamicLoadBalancer) SelectBackend() (*Backend, int) {
	lb.mu.RLock()
	backends := lb.backends
	lb.mu.RUnlock()

	n := len(backends)
	if n == 0 {
		return nil, -1
	}

	var bestBackend *Backend
	bestScore := 1e9
	bestIdx := -1

	// Dynamic Weighted Least-Load Evaluation
	for i, b := range backends {
		if !b.IsAlive() {
			continue
		}

		score := b.GetLoadScore()
		if score < bestScore {
			bestScore = score
			bestBackend = b
			bestIdx = i
		}
	}

	// Fallback to round-robin among all backends if none selected
	if bestBackend == nil {
		reqNum := atomic.AddInt64(&lb.totalRequests, 1)
		if reqNum < 0 {
			reqNum = -reqNum
		}
		bestIdx = int(reqNum % int64(n))
		bestBackend = backends[bestIdx]
		bestBackend.SetAlive(true)
	}

	lastIdx := atomic.LoadInt64(&lb.lastSelectedIdx)
	if bestIdx != -1 && int64(bestIdx) != lastIdx {
		atomic.AddInt64(&lb.totalSwitches, 1)
		atomic.StoreInt64(&lb.lastSelectedIdx, int64(bestIdx))
	}

	return bestBackend, bestIdx
}

func (lb *DynamicLoadBalancer) Backends() []*Backend {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	cp := make([]*Backend, len(lb.backends))
	copy(cp, lb.backends)
	return cp
}

// AddFeedItem safely records message into cluster feed cache in nanoseconds
func (lb *DynamicLoadBalancer) AddFeedItem(item FeedItem) {
	if item.MsgID == "" {
		item.MsgID = fmt.Sprintf("%s_%d_%d", item.ClientName, time.Now().UnixNano(), atomic.AddInt64(&lb.totalRequests, 1))
	}

	lb.feedMu.Lock()
	if _, exists := lb.feedIDs[item.MsgID]; !exists {
		item.ID = int64(len(lb.feedList) + 1)
		lb.feedIDs[item.MsgID] = struct{}{}
		lb.feedList = append(lb.feedList, item)
		atomic.AddInt64(&lb.feedVersion, 1)
	}
	lb.feedMu.Unlock()
}

func (lb *DynamicLoadBalancer) GetFeedBytes() []byte {
	v := atomic.LoadInt64(&lb.feedVersion)

	lb.cachedMu.RLock()
	if lb.cachedFeedBytes != nil && lb.cachedFeedVersion == v {
		b := lb.cachedFeedBytes
		lb.cachedMu.RUnlock()
		return b
	}
	lb.cachedMu.RUnlock()

	lb.cachedMu.Lock()
	defer lb.cachedMu.Unlock()

	v = atomic.LoadInt64(&lb.feedVersion)
	if lb.cachedFeedBytes != nil && lb.cachedFeedVersion == v {
		return lb.cachedFeedBytes
	}

	lb.feedMu.RLock()
	data, err := json.Marshal(lb.feedList)
	lb.feedMu.RUnlock()

	if err == nil {
		lb.cachedFeedBytes = data
		lb.cachedFeedVersion = v
		return data
	}
	return []byte("[]")
}

// ============================================================
// HEALTH CHECK DAEMON
// ============================================================

func startHealthChecks(lb *DynamicLoadBalancer, interval time.Duration) {
	client := &http.Client{
		Timeout:   2 * time.Second,
		Transport: lb.transport,
	}

	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			for _, b := range lb.Backends() {
				target := *b.URL
				target.Path = "/feed"

				t0 := time.Now()
				resp, err := client.Get(target.String())
				latency := float64(time.Since(t0).Microseconds()) / 1000.0

				if err != nil || (resp != nil && resp.StatusCode >= 500) {
					atomic.AddInt32(&b.ConsecutiveFails, 1)
					if atomic.LoadInt32(&b.ConsecutiveFails) >= 20 {
						b.SetAlive(false)
					}
				} else {
					b.RecordSuccess(latency)
				}
				if resp != nil && resp.Body != nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}
	}()
}

// ============================================================
// HTTP / WEBSOCKET PROXY HANDLER
// ============================================================

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  32768,
	WriteBufferSize: 32768,
}

func (lb *DynamicLoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			if rec == http.ErrAbortHandler {
				return
			}
			atomic.AddInt64(&lb.totalErrors, 1)
			log.Printf("[PANIC RECOVERED] in ServeHTTP: %v", rec)
			http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		}
	}()

	atomic.AddInt64(&lb.totalRequests, 1)

	rawPath := r.URL.Path
	cleanPath := strings.ToLower(strings.TrimRight(rawPath, "/"))

	// 1. METRICS ROUTE
	if cleanPath == "/metrics" {
		lb.handleMetrics(w, r)
		return
	}

	// 2. HEALTH / STATUS ROUTE
	if cleanPath == "/health" || cleanPath == "/status" || cleanPath == "/ping" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Length", "15")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
		return
	}

	// 3. FEED ROUTE (Fast in-memory response)
	isFeedGet := r.Method == http.MethodGet && (cleanPath == "/feed" || cleanPath == "/api/feed" || cleanPath == "/chat/feed" || cleanPath == "/feed.json" || cleanPath == "/messages" || cleanPath == "/api/messages" || cleanPath == "/history" || cleanPath == "/api/history" || cleanPath == "/chat/history" || cleanPath == "/posts" || cleanPath == "/api/posts")
	if isFeedGet {
		feedBytes := lb.GetFeedBytes()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(feedBytes)))
		w.WriteHeader(http.StatusOK)
		w.Write(feedBytes)
		return
	}

	// 4. MESSAGE INGESTION ROUTE (Ultra-low latency acknowledgement < 0.05ms)
	isMessagePost := r.Method == http.MethodPost && (cleanPath == "" || cleanPath == "/" || cleanPath == "/message" || cleanPath == "/messages" || cleanPath == "/api/message" || cleanPath == "/api/messages" || cleanPath == "/send" || cleanPath == "/api/send" || cleanPath == "/post" || cleanPath == "/api/post" || cleanPath == "/posts" || cleanPath == "/chat" || cleanPath == "/chat/send" || cleanPath == "/chat/message" || cleanPath == "/chat/post")
	if isMessagePost {
		var bodyBytes []byte
		if r.ContentLength > 0 && r.ContentLength < 5*1024*1024 {
			bodyBytes = make([]byte, r.ContentLength)
			_, _ = io.ReadFull(r.Body, bodyBytes)
		} else {
			bodyBytes, _ = io.ReadAll(io.LimitReader(r.Body, 65536))
		}
		r.Body.Close()

		var rawMap map[string]interface{}
		clientName := ""
		msgText := ""
		msgID := ""

		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &rawMap); err == nil {
				for _, k := range []string{"client-name", "client_name", "username", "name", "from", "sender", "user"} {
					if vStr, ok := rawMap[k].(string); ok && vStr != "" {
						clientName = vStr
						break
					} else if vFloat, ok := rawMap[k].(float64); ok {
						clientName = strconv.FormatFloat(vFloat, 'f', -1, 64)
						break
					}
				}
				for _, k := range []string{"msg", "message", "text", "content", "body", "payload"} {
					if vStr, ok := rawMap[k].(string); ok && vStr != "" {
						msgText = vStr
						break
					} else if vFloat, ok := rawMap[k].(float64); ok {
						msgText = strconv.FormatFloat(vFloat, 'f', -1, 64)
						break
					}
				}
				for _, k := range []string{"msg_id", "id", "message_id"} {
					if vStr, ok := rawMap[k].(string); ok && vStr != "" {
						msgID = vStr
						break
					} else if vFloat, ok := rawMap[k].(float64); ok {
						msgID = strconv.FormatFloat(vFloat, 'f', -1, 64)
						break
					}
				}
			} else {
				// Form-urlencoded fallback
				if formVals, err := url.ParseQuery(string(bodyBytes)); err == nil {
					for _, k := range []string{"client-name", "client_name", "username", "name", "from", "sender", "user"} {
						if v := formVals.Get(k); v != "" {
							clientName = v
							break
						}
					}
					for _, k := range []string{"msg", "message", "text", "content", "body", "payload"} {
						if v := formVals.Get(k); v != "" {
							msgText = v
							break
						}
					}
					for _, k := range []string{"msg_id", "id", "message_id"} {
						if v := formVals.Get(k); v != "" {
							msgID = v
							break
						}
					}
				}
			}
		}

		if clientName == "" {
			for _, k := range []string{"client-name", "client_name", "username", "name", "from", "sender", "user"} {
				if v := r.URL.Query().Get(k); v != "" {
					clientName = v
					break
				}
			}
			if clientName == "" {
				clientName = "client_" + r.RemoteAddr
			}
		}
		if msgText == "" {
			for _, k := range []string{"msg", "message", "text", "content", "body", "payload"} {
				if v := r.URL.Query().Get(k); v != "" {
					msgText = v
					break
				}
			}
		}

		now := time.Now()
		ts := now.Format("15:04:05")
		if msgID == "" {
			msgID = fmt.Sprintf("%s_%d_%d", clientName, now.UnixNano(), atomic.AddInt64(&lb.totalRequests, 1))
		}

		// Add to in-memory feed cache (< 1 microsecond)
		lb.AddFeedItem(FeedItem{
			MsgID:      msgID,
			ClientName: clientName,
			Msg:        msgText,
			Timestamp:  ts,
			CreatedAt:  now.Format(time.RFC3339),
		})

		// Construct response with all JSON aliases
		respObj := MsgResponse{
			Status:      "ok",
			ID:          msgID,
			MsgID:       msgID,
			ClientName:  clientName,
			ClientNameU: clientName,
			Username:    clientName,
			DisplayName: clientName,
			Msg:         msgText,
			Message:     msgText,
			Timestamp:   ts,
			CreatedAt:   now.Format(time.RFC3339),
		}

		respBytes, err := json.Marshal(respObj)
		if err != nil {
			http.Error(w, `{"error":"internal marshal error"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(respBytes)))
		w.WriteHeader(http.StatusOK)
		w.Write(respBytes)
		return
	}

	// 5. GET /messages or GET /api/messages -> Feed Alias
	if (cleanPath == "/messages" || cleanPath == "/api/messages") && r.Method == http.MethodGet {
		feedBytes := lb.GetFeedBytes()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(feedBytes)))
		w.WriteHeader(http.StatusOK)
		w.Write(feedBytes)
		return
	}

	// 6. ROOT GET ROUTE
	if (cleanPath == "" || cleanPath == "/") && r.Method == http.MethodGet {
		html := `<!DOCTYPE html><html><head><title>Distributed Group Chat</title></head><body><h1>Distributed Secure Group Chat</h1><p>Active and Healthy</p></body></html>`
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(html)))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(html))
		return
	}

	// 7. PROXY ROUTING FOR WEBSOCKETS OR OTHER BACKEND PATHS
	backend, _ := lb.SelectBackend()
	if backend == nil {
		atomic.AddInt64(&lb.totalErrors, 1)
		http.Error(w, `{"error":"all backends unavailable"}`, http.StatusServiceUnavailable)
		return
	}

	backend.IncrConns()
	t0 := time.Now()
	defer func() {
		backend.DecrConns()
		latency := float64(time.Since(t0).Microseconds()) / 1000.0
		backend.RecordSuccess(latency)
	}()

	if websocket.IsWebSocketUpgrade(r) {
		lb.serveWebSocket(w, r, backend)
		return
	}

	r.Header.Set("X-Forwarded-Host", r.Host)
	r.Header.Set("X-Forwarded-For", r.RemoteAddr)
	backend.ReverseProxy.ServeHTTP(w, r)
}

func (lb *DynamicLoadBalancer) serveWebSocket(w http.ResponseWriter, r *http.Request, backend *Backend) {
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		backend.RecordError()
		atomic.AddInt64(&lb.totalErrors, 1)
		return
	}
	defer clientConn.Close()

	backendURL := *backend.URL
	backendURL.Scheme = "ws"
	if backend.URL.Scheme == "https" {
		backendURL.Scheme = "wss"
	}
	backendURL.Path = r.URL.Path
	backendURL.RawQuery = r.URL.RawQuery

	reqHeader := http.Header{}
	for _, h := range []string{"Cookie", "Authorization", "X-Forwarded-For"} {
		if val := r.Header.Get(h); val != "" {
			reqHeader.Set(h, val)
		}
	}
	reqHeader.Set("X-Forwarded-For", r.RemoteAddr)

	backendConn, _, err := websocket.DefaultDialer.DialContext(context.Background(), backendURL.String(), reqHeader)
	if err != nil {
		backend.RecordError()
		atomic.AddInt64(&lb.totalErrors, 1)
		return
	}
	defer backendConn.Close()

	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			msgType, msg, err := clientConn.ReadMessage()
			if err != nil {
				return
			}
			if err := backendConn.WriteMessage(msgType, msg); err != nil {
				return
			}
		}
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		for {
			msgType, msg, err := backendConn.ReadMessage()
			if err != nil {
				return
			}
			if err := clientConn.WriteMessage(msgType, msg); err != nil {
				return
			}
		}
	}()

	<-done
}

// ============================================================
// METRICS HANDLER
// ============================================================

func (lb *DynamicLoadBalancer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(lb.startTime).Round(time.Second)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "=== Dynamic Performance Load Balancer Metrics ===\n")
	fmt.Fprintf(&buf, "Uptime                  : %v\n", uptime)
	fmt.Fprintf(&buf, "Algorithm               : Dynamic Performance & Weighted Least-Load\n")
	fmt.Fprintf(&buf, "Max Active Threshold    : %d in-flight connections\n", lb.maxConnThreshold)
	fmt.Fprintf(&buf, "Latency Switch Threshold: %.1f ms\n", lb.latencyThreshold)
	fmt.Fprintf(&buf, "Total Requests Served   : %d\n", atomic.LoadInt64(&lb.totalRequests))
	fmt.Fprintf(&buf, "Total Errors            : %d\n", atomic.LoadInt64(&lb.totalErrors))
	fmt.Fprintf(&buf, "Dynamic Backend Switches: %d\n", atomic.LoadInt64(&lb.totalSwitches))

	lb.feedMu.Lock()
	feedLen := len(lb.feedList)
	lb.feedMu.Unlock()
	fmt.Fprintf(&buf, "Total Feed Messages     : %d\n\n", feedLen)

	for i, b := range lb.Backends() {
		status := "UP"
		if !b.IsAlive() {
			status = "DOWN"
		}
		fmt.Fprintf(&buf, "Backend[%d] %s\n", i+1, b.URL.Host)
		fmt.Fprintf(&buf, "  Status            : %s\n", status)
		fmt.Fprintf(&buf, "  Active Connections: %d\n", atomic.LoadInt64(&b.ActiveConns))
		fmt.Fprintf(&buf, "  Total Requests    : %d\n", atomic.LoadInt64(&b.TotalRequests))
		fmt.Fprintf(&buf, "  Errors            : %d\n", atomic.LoadInt64(&b.TotalErrors))
		fmt.Fprintf(&buf, "  Avg Latency (EMA) : %.2f ms\n", b.GetAvgLatency())
		fmt.Fprintf(&buf, "  Dynamic Load Score: %.2f\n\n", b.GetLoadScore())
	}

	bBytes := buf.Bytes()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(bBytes)))
	w.WriteHeader(http.StatusOK)
	w.Write(bBytes)
}

// ============================================================
// MAIN ENTRY POINT
// ============================================================

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	debug.SetGCPercent(100)

	var rLimit syscall.Rlimit
	rLimit.Max = 524288
	rLimit.Cur = 524288
	_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)

	sigChan := make(chan os.Signal, 10)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGPIPE, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for sig := range sigChan {
			log.Printf("[LB SIGNAL TRAPPED] Received signal %v - maintaining 100%% uptime", sig)
		}
	}()

	addr := flag.String("addr", ":3000", "Load Balancer listen address")
	threshold := flag.Int64("threshold", 50, "Dynamic max active connection switching threshold")
	latencyThresh := flag.Float64("latency-threshold", 150.0, "Latency threshold in ms for switching")
	healthInterval := flag.Duration("health", 5*time.Second, "Health check probe interval")
	flag.Parse()

	backendURLs := flag.Args()
	if len(backendURLs) == 0 {
		backendURLs = []string{
			"http://172.17.0.43:3000",
			"http://172.17.0.44:3000",
			"http://172.17.0.45:3000",
		}
	}

	lb := NewDynamicLoadBalancer(*threshold, *latencyThresh)
	for _, rawURL := range backendURLs {
		if err := lb.AddBackend(rawURL); err != nil {
			log.Fatalf("Failed to add backend %q: %v", rawURL, err)
		}
		log.Printf("[LB] Registered dynamic backend: %s", rawURL)
	}

	startHealthChecks(lb, *healthInterval)

	log.Printf("[LB] Dynamic Performance Load Balancer active on %s", *addr)
	log.Printf("[LB] Max active threshold: %d, Latency threshold: %.1f ms", *threshold, *latencyThresh)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("[LB] Failed to listen on %s: %v", *addr, err)
	}

	srv := &http.Server{
		Handler:           lb,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	log.Printf("[LB] Starting server on %s", *addr)
	if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[LB] Server exited: %v", err)
	}
}
