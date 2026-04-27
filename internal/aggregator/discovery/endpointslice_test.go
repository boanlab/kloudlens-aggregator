// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func ptrTrue() *bool  { b := true; return &b }
func ptrFalse() *bool { b := false; return &b }

func TestFlattenFiltersUnready(t *testing.T) {
	state := map[string]sliceObject{
		"svc-x": {
			Metadata: struct {
				Name string `json:"name"`
			}{Name: "svc-x"},
			AddressType: "IPv4",
			Ports:       []slicePort{{Port: 9090}},
			Endpoints: []sliceEndpoint{
				{Addresses: []string{"10.0.0.1"}, Conditions: struct {
					Ready       *bool `json:"ready"`
					Serving     *bool `json:"serving"`
					Terminating *bool `json:"terminating"`
				}{Ready: ptrTrue()}},
				{Addresses: []string{"10.0.0.2"}, Conditions: struct {
					Ready       *bool `json:"ready"`
					Serving     *bool `json:"serving"`
					Terminating *bool `json:"terminating"`
				}{Ready: ptrFalse()}},
				// Missing Ready: assumed ready.
				{Addresses: []string{"10.0.0.3"}},
			},
		},
	}
	got := flatten(state, 0)
	if len(got) != 2 {
		t.Fatalf("flatten produced %d endpoints, want 2 (unready should be filtered): %+v", len(got), got)
	}
	// Sorted by addr.
	if got[0].Addr != "10.0.0.1:9090" || got[1].Addr != "10.0.0.3:9090" {
		t.Errorf("flatten result wrong: %+v", got)
	}
}

func TestFlattenPicksTargetPort(t *testing.T) {
	state := map[string]sliceObject{
		"svc": {
			Metadata: struct {
				Name string `json:"name"`
			}{Name: "svc"},
			Ports: []slicePort{{Port: 8080, Name: "http"}, {Port: 9090, Name: "grpc"}},
			Endpoints: []sliceEndpoint{
				{Addresses: []string{"10.1.1.1"}, Conditions: struct {
					Ready       *bool `json:"ready"`
					Serving     *bool `json:"serving"`
					Terminating *bool `json:"terminating"`
				}{Ready: ptrTrue()}},
			},
		},
	}
	got := flatten(state, 9090)
	if len(got) != 1 || got[0].Addr != "10.1.1.1:9090" {
		t.Errorf("TargetPort=9090 not honored: %+v", got)
	}
	// Missing target port → no endpoints.
	got = flatten(state, 7777)
	if len(got) != 0 {
		t.Errorf("unknown TargetPort should drop slice, got %+v", got)
	}
}

// TestWatcherListAndEmit exercises the HTTP list path against a test server
// serving one EndpointSlice, and verifies the watcher emits the expected
// Endpoint slice on the output channel.
func TestWatcherListAndEmit(t *testing.T) {
	slice := sliceObject{
		Metadata: struct {
			Name string `json:"name"`
		}{Name: "agents-1"},
		AddressType: "IPv4",
		Ports:       []slicePort{{Port: 9090}},
		Endpoints: []sliceEndpoint{
			{Addresses: []string{"10.0.0.1"}, Conditions: struct {
				Ready       *bool `json:"ready"`
				Serving     *bool `json:"serving"`
				Terminating *bool `json:"terminating"`
			}{Ready: ptrTrue()}},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" {
			// Block until the client disconnects — the emit must have
			// happened from the initial list.
			<-r.Context().Done()
			return
		}
		// LIST.
		if !strings.Contains(r.URL.RawQuery, "kubernetes.io%2Fservice-name%3Dagents") {
			t.Errorf("unexpected list URL: %s", r.URL.String())
		}
		resp := sliceList{Items: []sliceObject{slice}}
		resp.Metadata.ResourceVersion = "42"
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	// Token file — a temp path is fine for the Bearer.
	tokFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokFile, []byte("fake-token"), 0o600); err != nil {
		t.Fatal(err)
	}

	w := &EndpointSliceWatcher{
		APIServer:   srv.URL,
		TokenFile:   tokFile,
		Namespace:   "default",
		ServiceName: "agents",
		HTTPClient:  srv.Client(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := w.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case eps := <-ch:
		if len(eps) != 1 || eps[0].Addr != "10.0.0.1:9090" {
			t.Errorf("unexpected endpoints: %+v", eps)
		}
	case <-time.After(time.Second):
		t.Fatal("no emit from watcher within 1s")
	}
}
