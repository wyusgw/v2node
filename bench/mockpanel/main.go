// Command mockpanel is a minimal stand-in for the v2board UniProxy API, used
// only by the benchmarks under bench/. It serves one node config and a
// synthetic user list whose UUIDs follow bench/internal/users, and accepts
// (and counts) traffic/alive reports.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/wyusgw/v2node/bench/internal/users"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:18080", "panel listen address")
	protocol := flag.String("protocol", "vless", "vless | vmess | trojan | shadowsocks")
	port := flag.Int("port", 20443, "node server_port")
	n := flag.Int("users", 1000, "number of users")
	cipher := flag.String("cipher", "aes-128-gcm", "shadowsocks cipher")
	speed := flag.Int("speed", 0, "per-user speed limit in Mbps (0 = unlimited)")
	device := flag.Int("device", 0, "per-user device limit (0 = unlimited)")
	push := flag.Int("push", 10, "push_interval seconds")
	pull := flag.Int("pull", 60, "pull_interval seconds")
	flag.Parse()

	cfg, _ := json.Marshal(map[string]any{
		"protocol":    *protocol,
		"listen_ip":   "127.0.0.1",
		"server_port": *port,
		"network":     "tcp",
		"tls":         0,
		"cipher":      *cipher,
		"routes":      []any{},
		"base_config": map[string]any{
			"push_interval": *push,
			"pull_interval": *pull,
		},
	})

	type user struct {
		ID          int    `json:"id"`
		UUID        string `json:"uuid"`
		SpeedLimit  int    `json:"speed_limit"`
		DeviceLimit int    `json:"device_limit"`
	}
	list := make([]user, *n)
	for i := range list {
		list[i] = user{ID: i + 1, UUID: users.UUID(i), SpeedLimit: *speed, DeviceLimit: *device}
	}
	userBody, _ := json.Marshal(map[string]any{"users": list})
	userEtag := strconv.Quote(fmt.Sprintf("u%d-%d-%d", *n, *speed, *device))

	var pushes, pushedUsers, alives atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/server/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cfg)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/user", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == userEtag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", userEtag)
		w.Header().Set("Content-Type", "application/json")
		w.Write(userBody)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/alivelist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"alive":{}}`))
	})
	mux.HandleFunc("/api/v1/server/UniProxy/push", func(w http.ResponseWriter, r *http.Request) {
		var m map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&m)
		pushes.Add(1)
		pushedUsers.Add(int64(len(m)))
		w.Write([]byte(`{"data":true}`))
	})
	mux.HandleFunc("/api/v1/server/UniProxy/alive", func(w http.ResponseWriter, r *http.Request) {
		alives.Add(1)
		w.Write([]byte(`{"data":true}`))
	})
	mux.HandleFunc("/api/v1/server/UniProxy/claimDevice", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"allow":true}`))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "pushes=%d pushed_users=%d alive_reports=%d\n", pushes.Load(), pushedUsers.Load(), alives.Load())
	})
	log.Printf("mockpanel %s on %s: %s node port %d, %d users", *protocol, *listen, *protocol, *port, *n)
	log.Fatal(http.ListenAndServe(*listen, mux))
}
