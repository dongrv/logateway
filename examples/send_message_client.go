// Command send_message_client sends signed test messages to logateway.
//
// Example:
//
//	go run ./examples/send_message_client.go
//	go run ./examples/send_message_client.go -count 100 -concurrency 8
//	go run ./examples/send_message_client.go -project blogs -router Test -data '{"UID":42,"action":"view"}'
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type uploadRequest struct {
	Project string          `json:"project"`
	Router  string          `json:"router"`
	Data    json.RawMessage `json:"data"`
}

type uploadResponse struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id,omitempty"`
}

func main() {
	var (
		url         = flag.String("url", envOrDefault("LOGATEWAY_URL", "http://127.0.0.1:8080/api/v1/log/upload"), "upload endpoint")
		appKey      = flag.String("app-key", envOrDefault("LOGATEWAY_APP_KEY", "test-app-key"), "HMAC app key")
		secret      = flag.String("secret", envOrDefault("LOGATEWAY_SECRET", "test-secret"), "HMAC secret")
		project     = flag.String("project", envOrDefault("LOGATEWAY_PROJECT", "actilogs"), "project name")
		router      = flag.String("router", envOrDefault("LOGATEWAY_ROUTER", "CH=Behavior"), "router value")
		data        = flag.String("data", `{"UID":123,"action":"click","source":"example-client"}`, "JSON object/array/string used as data")
		count       = flag.Int("count", 1, "number of messages to send")
		concurrency = flag.Int("concurrency", 1, "concurrent workers")
		timeout     = flag.Duration("timeout", 5*time.Second, "per-request timeout")
	)
	flag.Parse()

	if *count <= 0 {
		log.Fatal("-count must be greater than 0")
	}
	if *concurrency <= 0 {
		log.Fatal("-concurrency must be greater than 0")
	}

	rawData := json.RawMessage(strings.TrimSpace(*data))
	if !json.Valid(rawData) {
		log.Fatalf("-data is not valid JSON: %s", *data)
	}

	client := &http.Client{Timeout: *timeout}
	jobs := make(chan int)
	var ok, failed atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for seq := range jobs {
				resp, err := sendOne(context.Background(), client, *url, *appKey, *secret, *project, *router, rawData, seq)
				if err != nil {
					failed.Add(1)
					log.Printf("[worker=%d seq=%d] send failed: %v", workerID, seq, err)
					continue
				}
				ok.Add(1)
				log.Printf("[worker=%d seq=%d] ok request_id=%s trace_id=%s message=%s",
					workerID, seq, resp.RequestID, resp.TraceID, resp.Message)
			}
		}(i)
	}

	for i := 0; i < *count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	fmt.Printf("done: success=%d failed=%d duration=%s endpoint=%s\n",
		ok.Load(), failed.Load(), time.Since(start).Round(time.Millisecond), *url)
	if failed.Load() > 0 {
		os.Exit(1)
	}
}

func sendOne(ctx context.Context, client *http.Client, url, appKey, secret, project, router string, data json.RawMessage, seq int) (*uploadResponse, error) {
	body, err := json.Marshal(uploadRequest{
		Project: project,
		Router:  router,
		Data:    withSequence(data, seq),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	nonce := newNonce(seq)
	signature := sign(secret, body, timestamp, nonce)
	requestID := newNonce(seq)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Key", appKey)
	req.Header.Set("X-Timestamp", timestamp)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signature)
	req.Header.Set("X-Request-Id", requestID)
	req.Header.Set("X-Trace-Id", requestID)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var out uploadResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("decode response: %w body=%s", err, strings.TrimSpace(string(respBody)))
	}
	if out.Code != 0 {
		return nil, fmt.Errorf("gateway code=%d message=%s", out.Code, out.Message)
	}
	return &out, nil
}

func sign(secret string, body []byte, timestamp, nonce string) string {
	data := make([]byte, 0, len(body)+len(timestamp)+len(nonce))
	data = append(data, body...)
	data = append(data, timestamp...)
	data = append(data, nonce...)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func withSequence(raw json.RawMessage, seq int) json.RawMessage {
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	obj["_example_seq"] = seq
	obj["_example_sent_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

func newNonce(seq int) string {
	return fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), seq, rand.Int63())
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
