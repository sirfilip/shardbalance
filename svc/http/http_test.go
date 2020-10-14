package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func assertEqualJSON(t *testing.T, expectedJSON, gotJSON []byte) {
	t.Helper()
	var expected, got map[string]string
	var err error
	err = json.Unmarshal(expectedJSON, &expected)
	if err != nil {
		t.Error(err)
	}
	err = json.Unmarshal(gotJSON, &got)
	if len(expected) != len(got) {
		t.Errorf("Expected %v but got %v", expected, got)
	}
	for key, exp := range expected {
		if val, ok := got[key]; !ok || exp != val {
			t.Errorf("Expected %v but got %v", expected, got)
		}
	}
}

func TestHTTPServer(t *testing.T) {
	for title, test := range map[string]struct {
		history []struct {
			request *http.Request
			headers map[string]string
		}
		request *http.Request
		headers map[string]string
		status  int
		body    []byte
	}{
		"getting addrs when no shards present": {
			request: httptest.NewRequest(http.MethodGet, "/shardkey", nil),
			status:  404,
			body:    []byte(`{}`),
		},
		"getting addrs when shard exists": {
			history: []struct {
				request *http.Request
				headers map[string]string
			}{
				{
					request: httptest.NewRequest(http.MethodPost, "/shards", strings.NewReader(`address=example`)),
					headers: map[string]string{
						"Content-Type": "application/x-www-form-urlencoded",
					},
				},
			},
			request: httptest.NewRequest(http.MethodGet, "/shardkey", nil),
			status:  200,
			body:    []byte(`{"address": "example"}`),
		},
		"creating shard without addr specified": {
			request: httptest.NewRequest(http.MethodPost, "/shards", strings.NewReader(``)),
			status:  400,
			body:    []byte(`{"error": "address is required"}`),
		},
		"creating shard with valid addres specified": {
			headers: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
			request: httptest.NewRequest(http.MethodPost, "/shards", strings.NewReader(`address=example`)),
			status:  201,
			body:    []byte(`{}`),
		},
		"creating shard with existing address": {
			history: []struct {
				request *http.Request
				headers map[string]string
			}{
				{
					request: httptest.NewRequest(http.MethodPost, "/shards", strings.NewReader(`address=example`)),
					headers: map[string]string{
						"Content-Type": "application/x-www-form-urlencoded",
					},
				},
			},
			headers: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
			request: httptest.NewRequest(http.MethodPost, "/shards", strings.NewReader(`address=example`)),
			status:  400,
			body:    []byte(`{"error": "shard already exists"}`),
		},
		"deleting shard with non existing address": {
			request: httptest.NewRequest(http.MethodDelete, "/shards/example", nil),
			status:  400,
			body:    []byte(`{"error": "not found"}`),
		},
		"deleting shard with existing address": {
			history: []struct {
				request *http.Request
				headers map[string]string
			}{
				{
					request: httptest.NewRequest(http.MethodPost, "/shards", strings.NewReader(`address=example`)),
					headers: map[string]string{
						"Content-Type": "application/x-www-form-urlencoded",
					},
				},
			},
			request: httptest.NewRequest(http.MethodDelete, "/shards/example", nil),
			status:  200,
			body:    []byte(`{}`),
		},
	} {
		t.Run(title, func(t *testing.T) {
			r := createServer(42)
			w := httptest.NewRecorder()
			for _, h := range test.history {
				for header, value := range h.headers {
					h.request.Header.Add(header, value)
				}
				r.ServeHTTP(httptest.NewRecorder(), h.request)
			}
			for header, value := range test.headers {
				test.request.Header.Add(header, value)
			}
			r.ServeHTTP(w, test.request)
			if w.Code != test.status {
				t.Errorf("Expected status %v but got %v", test.status, w.Code)
			}
			assertEqualJSON(t, test.body, w.Body.Bytes())
		})
	}
}
