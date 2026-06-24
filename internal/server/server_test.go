package server

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/dongrv/logateway/internal/message"
)

func signBody(secret, body, timestamp, nonce string) string {
	data := body + timestamp + nonce
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func signedRequest(url, body, appKey, secret, noncePrefix string) (*http.Request, error) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := fmt.Sprintf("%s-%d", noncePrefix, time.Now().UnixNano())
	sig := signBody(secret, body, ts, nonce)

	req, err := http.NewRequest("POST", url, bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Key", appKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)
	return req, nil
}

func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	gw, err := New("../../configs/gateway.yaml")
	if err != nil {
		t.Fatalf("gateway init: %v", err)
	}
	t.Cleanup(gw.Close)

	return httptest.NewServer(gw.Router)
}

func TestUploadSuccess(t *testing.T) {
	srv := testServer(t)
	defer srv.Close()

	body := `{"project":"actilogs","router":"CH=Behavior","data":{"UID":123,"action":"click"}}`
	req, err := signedRequest(srv.URL+"/api/v1/log/upload", body,
		"test-app-key", "test-secret", "success")
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(bodyBytes))
	}

	var r message.UploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if r.Code != 0 {
		t.Errorf("code = %d, want 0", r.Code)
	}
	if r.RequestID == "" {
		t.Error("RequestID is empty")
	}
}

func TestUploadNoAuth(t *testing.T) {
	srv := testServer(t)
	defer srv.Close()

	body := `{"project":"actilogs","router":"Test","data":{"k":"v"}}`
	req, _ := http.NewRequest("POST", srv.URL+"/api/v1/log/upload", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestUploadUnknownProject(t *testing.T) {
	srv := testServer(t)
	defer srv.Close()

	body := `{"project":"nonexistent","router":"Test","data":{"k":"v"}}`
	req, _ := signedRequest(srv.URL+"/api/v1/log/upload", body,
		"test-app-key", "test-secret", "unknown")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestUploadMissingProject(t *testing.T) {
	srv := testServer(t)
	defer srv.Close()

	body := `{"router":"Test","data":{"k":"v"}}`
	req, _ := signedRequest(srv.URL+"/api/v1/log/upload", body,
		"test-app-key", "test-secret", "missproj")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestUploadNonceReplay(t *testing.T) {
	srv := testServer(t)
	defer srv.Close()

	body := `{"project":"actilogs","router":"Test","data":{"k":"v"}}`
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := fmt.Sprintf("replay-%d", time.Now().UnixNano())
	sig := signBody("test-secret", body, ts, nonce)

	do := func() int {
		req, _ := http.NewRequest("POST", srv.URL+"/api/v1/log/upload", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-App-Key", "test-app-key")
		req.Header.Set("X-Timestamp", ts)
		req.Header.Set("X-Nonce", nonce)
		req.Header.Set("X-Signature", sig)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := do(); code != http.StatusOK {
		t.Fatalf("first upload: %d, want 200", code)
	}
	if code := do(); code != http.StatusUnauthorized {
		t.Errorf("replay: %d, want 401", code)
	}
}

func TestUploadBadSignature(t *testing.T) {
	srv := testServer(t)
	defer srv.Close()

	body := `{"project":"actilogs","router":"Test","data":{"k":"v"}}`
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := fmt.Sprintf("badsig-%d", time.Now().UnixNano())

	req, _ := http.NewRequest("POST", srv.URL+"/api/v1/log/upload", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Key", "test-app-key")
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", "0000000000000000000000000000000000000000000000000000000000000000")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestHealthEndpoint(t *testing.T) {
	srv := testServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer resp.Body.Close()

	// May return 503 if Redis not available; just verify not 404
	if resp.StatusCode == http.StatusNotFound {
		t.Error("health endpoint not found")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := testServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}

	data, _ := io.ReadAll(resp.Body)
	if len(data) == 0 {
		t.Error("empty metrics body")
	}
}
