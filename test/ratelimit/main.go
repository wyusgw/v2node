// ratelimit runs the real v2node binary against a mock panel and checks the
// per-user speed limit, device limit and kick logic end to end over VLESS.
package main

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	bin  = flag.String("bin", "", "v2node binary")
	out  = flag.String("out", "results", "output dir")
	cpus = flag.String("cpus", "0-1", "taskset cpu list for v2node")
	only = flag.String("only", "", "comma separated scenario names to run")
)

const (
	panelPort = 28080
	nodePort  = 28443
	srcPort   = 29001 // writes forever
	sinkPort  = 29002 // reads an 8-byte id, then counts what it reads
	pprofPort = 26060
	pullSec   = 2
	pushSec   = 3
)

func mbps(m float64) int64 { return int64(m * 1e6 / 8) }

// ---------------------------------------------------------------- panel

type user struct {
	Id              int    `json:"id"`
	Uuid            string `json:"uuid"`
	SpeedLimit      int    `json:"speed_limit"`
	SpeedLimitBps   int64  `json:"speed_limit_bps"`
	SpeedLimitUpBps *int64 `json:"speed_limit_up_bps"`
	DeviceLimit     int    `json:"device_limit"`
}

type mockPanel struct {
	mu        sync.Mutex
	users     map[int]*user
	version   int
	kicks     map[int]map[string]int
	claimMode string // ok | error | slow
	claimed   map[int]map[string]bool
	claims    atomic.Int64
	pulls     atomic.Int64
}

var P = &mockPanel{users: map[int]*user{}, kicks: map[int]map[string]int{}, claimed: map[int]map[string]bool{}, claimMode: "ok"}

func uuidOf(i int) [16]byte {
	h := md5.Sum([]byte("rl-user-" + strconv.Itoa(i)))
	h[6] = h[6]&0x0f | 0x40
	h[8] = h[8]&0x3f | 0x80
	return h
}
func uuidStr(u [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// setUser sets uid's limits: down/up in bytes/s, up < 0 meaning "field absent"
// (old panel: one limit for both directions).
func setUser(uid int, down, up int64, dev int) {
	P.mu.Lock()
	defer P.mu.Unlock()
	u := &user{Id: uid, Uuid: uuidStr(uuidOf(uid)), SpeedLimitBps: down, DeviceLimit: dev}
	u.SpeedLimit = int((down*8 + 999999) / 1000000)
	if up >= 0 {
		u.SpeedLimitUpBps = &up
	}
	P.users[uid] = u
	P.version++
}

// setLegacyUser only sends speed_limit (whole Mbps).
func setLegacyUser(uid, m int) {
	P.mu.Lock()
	defer P.mu.Unlock()
	P.users[uid] = &user{Id: uid, Uuid: uuidStr(uuidOf(uid)), SpeedLimit: m}
	P.version++
}

func (p *mockPanel) serve() {
	ob, _ := json.Marshal(map[string]any{"tag": "rlout", "protocol": "freedom",
		"settings": map[string]any{"finalRules": []any{map[string]any{"action": "allow", "ip": []string{"127.0.0.1/32"}}}}})
	node := map[string]any{"protocol": *proto, "listen_ip": "127.0.0.1", "server_port": nodePort, "network": "tcp", "tls": 0,
		"routes":      []any{map[string]any{"id": 1, "match": []string{}, "action": "default_out", "action_value": string(ob)}},
		"base_config": map[string]any{"push_interval": pushSec, "pull_interval": pullSec}}
	if *proto != "vless" {
		node["tls"] = 1
		node["tls_settings"] = map[string]any{"cert_mode": "self", "server_name": "test.local"}
		node["congestion_control"] = "bbr"
	}
	cfg, _ := json.Marshal(node)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/server/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "cfg1")
		if r.Header.Get("If-None-Match") == "cfg1" {
			w.WriteHeader(304)
			return
		}
		w.Write(cfg)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/user", func(w http.ResponseWriter, r *http.Request) {
		p.pulls.Add(1)
		p.mu.Lock()
		etag := "u" + strconv.Itoa(p.version)
		if r.Header.Get("If-None-Match") == etag {
			p.mu.Unlock()
			w.WriteHeader(304)
			return
		}
		ids := make([]int, 0, len(p.users))
		for id := range p.users {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		list := make([]*user, 0, len(ids))
		for _, id := range ids {
			list = append(list, p.users[id])
		}
		b, _ := json.Marshal(map[string]any{"users": list})
		p.mu.Unlock()
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/alivelist", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		b, _ := json.Marshal(map[string]any{"alive": map[string]int{}, "kick": p.kicks})
		p.kicks = map[int]map[string]int{} // a kick is sent once
		p.mu.Unlock()
		w.Write(b)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/claimDevice", func(w http.ResponseWriter, r *http.Request) {
		p.claims.Add(1)
		var req struct {
			UID         int    `json:"uid"`
			IP          string `json:"ip"`
			DeviceLimit int    `json:"device_limit"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		p.mu.Lock()
		mode := p.claimMode
		p.mu.Unlock()
		switch mode {
		case "error":
			w.WriteHeader(500)
			return
		case "slow":
			time.Sleep(6 * time.Second)
		}
		p.mu.Lock()
		set := p.claimed[req.UID]
		if set == nil {
			set = map[string]bool{}
			p.claimed[req.UID] = set
		}
		allow := set[req.IP] || len(set) < req.DeviceLimit
		if allow {
			set[req.IP] = true
		}
		p.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]bool{"allow": allow})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"data":true}`)
	})
	go http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", panelPort), mux)
}

// ---------------------------------------------------------------- targets

var sinkCounters sync.Map // id uint64 -> *atomic.Int64

func sinkCounter(id uint64) *atomic.Int64 {
	v, _ := sinkCounters.LoadOrStore(id, new(atomic.Int64))
	return v.(*atomic.Int64)
}

func targets() {
	ls, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", srcPort))
	if err != nil {
		panic(err)
	}
	go func() {
		buf := make([]byte, 32<<10)
		for {
			c, err := ls.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				go io.Copy(io.Discard, c)
				for {
					if _, err := c.Write(buf); err != nil {
						return
					}
				}
			}()
		}
	}()
	lk, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", sinkPort))
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			c, err := lk.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				var idb [8]byte
				if _, err := io.ReadFull(c, idb[:]); err != nil {
					return
				}
				ctr := sinkCounter(binary.BigEndian.Uint64(idb[:]))
				buf := make([]byte, 32<<10)
				for {
					n, err := c.Read(buf)
					ctr.Add(int64(n))
					if err != nil {
						return
					}
				}
			}()
		}
	}()
}

// ---------------------------------------------------------------- client

var flowID atomic.Uint64

type flow struct {
	name       string
	ctr        *atomic.Int64 // bytes moved through the proxy
	accepted   chan bool     // true once the VLESS response arrived, false if rejected
	closed     atomic.Bool
	closedAt   atomic.Int64 // unix nano
	conn       net.Conn
	extra      io.Closer
	acceptOnce sync.Once
}

func (f *flow) stop() {
	if f.conn != nil {
		f.conn.Close()
	}
	if f.extra != nil {
		f.extra.Close()
	}
}

func dialFrom(srcIP string) (net.Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second, LocalAddr: &net.TCPAddr{IP: net.ParseIP(srcIP)}}
	return d.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", nodePort))
}

func vlessHeader(uid, port int) []byte {
	u := uuidOf(uid)
	h := []byte{0}
	h = append(h, u[:]...)
	h = append(h, 0, 1)
	h = binary.BigEndian.AppendUint16(h, uint16(port))
	return append(h, 1, 127, 0, 0, 1)
}

func markClosed(f *flow) {
	if !f.closed.Swap(true) {
		f.closedAt.Store(time.Now().UnixNano())
	}
}

// download pulls from the source target through uid. pace > 0 makes the
// client itself read no faster than pace bytes/s (used for the baseline).
func download(name string, uid int, srcIP string, pace int64) *flow {
	f := &flow{name: name, ctr: new(atomic.Int64), accepted: make(chan bool, 1)}
	c, vlessResp, err := openTCP(uid, srcIP, srcPort, []byte{'x'})
	if err != nil {
		setAccepted(f, false)
		markClosed(f)
		return f
	}
	f.conn = c
	go func() {
		defer markClosed(f)
		r := bufio.NewReaderSize(c, 64<<10)
		c.SetReadDeadline(time.Now().Add(8 * time.Second))
		if vlessResp {
			var h [2]byte
			if _, err := io.ReadFull(r, h[:]); err != nil {
				setAccepted(f, false)
				return
			}
			r.Discard(int(h[1]))
		} else if _, err := r.Peek(1); err != nil {
			// through a socks client the first byte of data is the acceptance
			setAccepted(f, false)
			return
		}
		c.SetReadDeadline(time.Time{})
		setAccepted(f, true)
		buf := make([]byte, 32<<10)
		start := time.Now()
		var got int64
		for {
			n, err := r.Read(buf)
			got += int64(n)
			f.ctr.Add(int64(n))
			if err != nil {
				return
			}
			if pace > 0 {
				ahead := time.Duration(float64(got)/float64(pace)*1e9) - time.Since(start)
				if ahead > 0 {
					time.Sleep(ahead)
				}
			}
		}
	}()
	return f
}

// upload pushes to the sink target through uid; the sink counts what arrives.
func upload(name string, uid int, srcIP string) *flow {
	id := flowID.Add(1)
	f := &flow{name: name, ctr: sinkCounter(id), accepted: make(chan bool, 1)}
	c, _, err := openTCP(uid, srcIP, sinkPort, binary.BigEndian.AppendUint64(nil, id))
	if err != nil {
		setAccepted(f, false)
		markClosed(f)
		return f
	}
	f.conn = c
	// accepted once bytes reach the sink; rejected if the node closes first
	go func() {
		for {
			if f.ctr.Load() > 0 {
				setAccepted(f, true)
				return
			}
			if f.closed.Load() {
				setAccepted(f, false)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	go func() { // the node closing the connection ends the read
		io.Copy(io.Discard, c)
		markClosed(f)
	}()
	go func() {
		defer markClosed(f)
		buf := make([]byte, 32<<10)
		for {
			if _, err := c.Write(buf); err != nil {
				return
			}
		}
	}()
	return f
}

// setAccepted records whether the flow got through; the first answer wins.
func setAccepted(f *flow, v bool) {
	f.acceptOnce.Do(func() { f.accepted <- v })
}

func acceptedWithin(f *flow, d time.Duration) bool {
	select {
	case a := <-f.accepted:
		f.accepted <- a // the channel was emptied above and is only written once
		return a
	case <-time.After(d):
		return false
	}
}

// ---------------------------------------------------------------- measurement

// series samples each flow's counter every 100 ms.
type series struct {
	flows []*flow
	t     []time.Time
	v     [][]int64
	stopc chan struct{}
	wg    sync.WaitGroup
}

func record(flows ...*flow) *series {
	s := &series{flows: flows, stopc: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		snap := func() {
			row := make([]int64, len(s.flows))
			for i, f := range s.flows {
				row[i] = f.ctr.Load()
			}
			s.t = append(s.t, time.Now())
			s.v = append(s.v, row)
		}
		snap()
		for {
			select {
			case <-s.stopc:
				snap()
				return
			case <-tk.C:
				snap()
			}
		}
	}()
	return s
}

func (s *series) stop() { close(s.stopc); s.wg.Wait() }

type stat struct {
	AvgMbps      float64 `json:"avg_mbps"`
	P5Mbps       float64 `json:"p5_100ms_mbps"`
	P95Mbps      float64 `json:"p95_100ms_mbps"`
	MaxMbps      float64 `json:"max_100ms_mbps"`
	FirstSecMbps float64 `json:"first_1s_mbps"`
	FirstWinMB   float64 `json:"first_100ms_mb"`
}

// stats of the sum of flows idx (all if nil) from skip seconds on.
func (s *series) stats(idx []int, skip float64) stat {
	if idx == nil {
		for i := range s.flows {
			idx = append(idx, i)
		}
	}
	sum := func(row []int64) int64 {
		var t int64
		for _, i := range idx {
			t += row[i]
		}
		return t
	}
	t0 := s.t[0]
	var wins []float64
	var st stat
	first, last := -1, len(s.t)-1
	for k := 1; k < len(s.t); k++ {
		dt := s.t[k].Sub(s.t[k-1]).Seconds()
		r := float64(sum(s.v[k])-sum(s.v[k-1])) * 8 / dt / 1e6
		if k == 1 {
			st.FirstWinMB = float64(sum(s.v[k])-sum(s.v[0])) / 1e6
		}
		// the final snapshot after stop can close a window of a few ms,
		// whose rate is meaningless
		if s.t[k].Sub(t0).Seconds() >= skip && dt >= 0.05 {
			if first < 0 {
				first = k - 1
			}
			wins = append(wins, r)
		}
	}
	// first second
	for k := 1; k < len(s.t); k++ {
		if s.t[k].Sub(t0) >= time.Second {
			st.FirstSecMbps = float64(sum(s.v[k])-sum(s.v[0])) * 8 / s.t[k].Sub(t0).Seconds() / 1e6
			break
		}
	}
	if first >= 0 && last > first {
		st.AvgMbps = float64(sum(s.v[last])-sum(s.v[first])) * 8 / s.t[last].Sub(s.t[first]).Seconds() / 1e6
	}
	sort.Float64s(wins)
	if len(wins) > 0 {
		st.P5Mbps = wins[len(wins)*5/100]
		st.P95Mbps = wins[len(wins)*95/100]
		st.MaxMbps = wins[len(wins)-1]
	}
	return st
}

// perSecond returns the summed Mbps of flows idx for each whole second.
func (s *series) perSecond(idx []int) []float64 {
	var outv []float64
	t0 := s.t[0]
	sum := func(row []int64) int64 {
		var t int64
		for _, i := range idx {
			t += row[i]
		}
		return t
	}
	prev := 0
	for k := 1; k < len(s.t); k++ {
		if s.t[k].Sub(t0) >= time.Duration(len(outv)+1)*time.Second {
			outv = append(outv, float64(sum(s.v[k])-sum(s.v[prev]))*8/s.t[k].Sub(s.t[prev]).Seconds()/1e6)
			prev = k
		}
	}
	return outv
}

func r1(v float64) float64 { return math.Round(v*10) / 10 }

// ---------------------------------------------------------------- process sampling

func procCPU(pid int) int64 {
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 {
		return 0
	}
	f := strings.Fields(s[i+2:])
	u, _ := strconv.ParseInt(f[11], 10, 64)
	k, _ := strconv.ParseInt(f[12], 10, 64)
	return u + k
}

func procRSS(pid int) float64 {
	st, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	for _, l := range strings.Split(string(st), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			v, _ := strconv.ParseFloat(strings.Fields(l)[1], 64)
			return v / 1024
		}
	}
	return 0
}

func fetch(url, file string) {
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	f, _ := os.Create(filepath.Join(*out, file))
	io.Copy(f, resp.Body)
	f.Close()
}

// ---------------------------------------------------------------- scenarios

var results = map[string]any{}
var nodePid int

func logf(format string, a ...any) { fmt.Printf(format+"\n", a...) }

// waitApplied waits for v2node to have pulled the user list at least twice
// after a change (one pull may already be in flight with the old list).
func waitApplied() {
	n := P.pulls.Load()
	for P.pulls.Load() < n+2 {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
}

func runFor(d time.Duration, flows ...*flow) *series {
	s := record(flows...)
	time.Sleep(d)
	s.stop()
	for _, f := range flows {
		f.stop()
	}
	return s
}

func scAccuracy() {
	type row struct {
		Dir       string  `json:"dir"`
		LimitMbps float64 `json:"limit_mbps"`
		stat
		ErrPct float64 `json:"err_pct"`
	}
	var rows []row
	limits := []float64{1, 5, 10, 50, 100, 500, 1000}
	for i, m := range limits {
		uid := 100 + i
		for _, dir := range []string{"down", "up"} {
			var f *flow
			if dir == "down" {
				f = download(dir, uid, "127.0.0.1", 0)
			} else {
				f = upload(dir, uid, "127.0.0.1")
			}
			if !acceptedWithin(f, 5*time.Second) {
				logf("accuracy %s %v: not accepted", dir, m)
				continue
			}
			s := runFor(8*time.Second, f)
			st := s.stats(nil, 2)
			r := row{Dir: dir, LimitMbps: m, stat: st, ErrPct: r1((st.AvgMbps/m - 1) * 100)}
			rows = append(rows, r)
			logf("accuracy %-4s limit %6.0f Mbps: avg %8.2f (%+.1f%%)  100ms p5 %7.1f p95 %7.1f max %7.1f  first-1s %7.1f  first-100ms %.2f MB",
				dir, m, st.AvgMbps, r.ErrPct, st.P5Mbps, st.P95Mbps, st.MaxMbps, st.FirstSecMbps, st.FirstWinMB)
			time.Sleep(300 * time.Millisecond)
		}
	}
	results["accuracy"] = rows
}

func scDirections() {
	res := map[string]any{}
	// separate up/down limits on one user, both directions at once
	fd := download("down", 200, "127.0.0.1", 0)
	fu := upload("up", 200, "127.0.0.1")
	acceptedWithin(fd, 5*time.Second)
	acceptedWithin(fu, 5*time.Second)
	s := runFor(8*time.Second, fd, fu)
	d, u := s.stats([]int{0}, 2), s.stats([]int{1}, 2)
	res["asymmetric_down20_up5"] = map[string]float64{"down_mbps": r1(d.AvgMbps), "up_mbps": r1(u.AvgMbps)}
	logf("asymmetric (down 20 / up 5 Mbps, simultaneous): down %.2f up %.2f", d.AvgMbps, u.AvgMbps)

	// legacy panel: speed_limit (Mbps) only
	fd = download("down", 201, "127.0.0.1", 0)
	fu = upload("up", 201, "127.0.0.1")
	acceptedWithin(fd, 5*time.Second)
	acceptedWithin(fu, 5*time.Second)
	s = runFor(8*time.Second, fd, fu)
	d, u = s.stats([]int{0}, 2), s.stats([]int{1}, 2)
	res["legacy_speed_limit_8"] = map[string]float64{"down_mbps": r1(d.AvgMbps), "up_mbps": r1(u.AvgMbps)}
	logf("legacy speed_limit=8 only: down %.2f up %.2f", d.AvgMbps, u.AvgMbps)

	// upload explicitly unlimited (speed_limit_up_bps = 0)
	fd = download("down", 202, "127.0.0.1", 0)
	fu = upload("up", 202, "127.0.0.1")
	acceptedWithin(fd, 5*time.Second)
	acceptedWithin(fu, 5*time.Second)
	s = runFor(6*time.Second, fd, fu)
	d, u = s.stats([]int{0}, 2), s.stats([]int{1}, 2)
	res["down10_up_unlimited"] = map[string]float64{"down_mbps": r1(d.AvgMbps), "up_mbps": r1(u.AvgMbps)}
	logf("down 10 / up unlimited: down %.2f up %.2f", d.AvgMbps, u.AvgMbps)
	results["directions"] = res
}

func scShared() {
	res := map[string]any{}
	for _, n := range []int{1, 4, 16, 64} {
		var fs []*flow
		for i := 0; i < n; i++ {
			fs = append(fs, download("d", 300, "127.0.0.1", 0))
		}
		for _, f := range fs {
			acceptedWithin(f, 5*time.Second)
		}
		s := runFor(8*time.Second, fs...)
		tot := s.stats(nil, 2)
		var per []float64
		for i := range fs {
			per = append(per, s.stats([]int{i}, 2).AvgMbps)
		}
		sort.Float64s(per)
		res[fmt.Sprintf("conns_%d", n)] = map[string]float64{"total_mbps": r1(tot.AvgMbps), "per_conn_min": r1(per[0]), "per_conn_max": r1(per[len(per)-1])}
		logf("one user 20 Mbps, %2d conns: total %.2f Mbps, per conn min %.2f max %.2f", n, tot.AvgMbps, per[0], per[len(per)-1])
		time.Sleep(300 * time.Millisecond)
	}
	// two devices of the same user
	a1 := download("a", 301, "127.0.0.2", 0)
	a2 := download("a", 301, "127.0.0.2", 0)
	b1 := download("b", 301, "127.0.0.3", 0)
	for _, f := range []*flow{a1, a2, b1} {
		acceptedWithin(f, 5*time.Second)
	}
	s := runFor(8*time.Second, a1, a2, b1)
	tot, da, db := s.stats(nil, 2), s.stats([]int{0, 1}, 2), s.stats([]int{2}, 2)
	res["two_devices"] = map[string]float64{"total_mbps": r1(tot.AvgMbps), "device_a_2conns": r1(da.AvgMbps), "device_b_1conn": r1(db.AvgMbps)}
	logf("one user 20 Mbps, device A 2 conns + device B 1 conn: total %.2f  A %.2f  B %.2f", tot.AvgMbps, da.AvgMbps, db.AvgMbps)
	results["shared"] = res
}

func scIsolation() {
	var fs []*flow
	for i := 0; i < 10; i++ {
		fs = append(fs, download("lim", 400+i, "127.0.0.1", 0))
	}
	fs = append(fs, download("free", 410, "127.0.0.1", 0))
	for _, f := range fs {
		acceptedWithin(f, 5*time.Second)
	}
	s := runFor(8*time.Second, fs...)
	var per []float64
	for i := 0; i < 10; i++ {
		per = append(per, r1(s.stats([]int{i}, 2).AvgMbps))
	}
	free := s.stats([]int{10}, 2).AvgMbps
	results["isolation"] = map[string]any{"limited_users_mbps": per, "unlimited_user_mbps": r1(free)}
	logf("10 users x 10 Mbps + 1 unlimited: limited %v, unlimited %.0f Mbps", per, free)
}

// scLive changes one user's limit while it downloads and follows the
// connection: limit->limit is applied live, limited<->unlimited drops it.
func scLive() {
	const uid = 500
	type event struct {
		T    float64 `json:"t"`
		What string  `json:"what"`
	}
	var events []event
	start := time.Now()
	ev := func(w string) {
		events = append(events, event{r1(time.Since(start).Seconds()), w})
		logf("  live t=%.1fs %s", time.Since(start).Seconds(), w)
	}
	total := new(atomic.Int64)
	agg := &flow{name: "agg", ctr: total}
	var cur atomic.Pointer[flow]
	stopc := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // keep one download open, reconnecting when it drops
		defer wg.Done()
		for {
			f := download("live", uid, "127.0.0.1", 0)
			cur.Store(f)
			acceptedWithin(f, 5*time.Second)
			var last int64
			for !f.closed.Load() {
				select {
				case <-stopc:
					f.stop()
					return
				case <-time.After(20 * time.Millisecond):
				}
				v := f.ctr.Load()
				total.Add(v - last)
				last = v
			}
			total.Add(f.ctr.Load() - last)
			ev("connection closed by node, reconnecting")
		}
	}()
	s := record(agg)
	ev("start at 10 Mbps")
	time.Sleep(6 * time.Second)
	setUser(uid, mbps(40), mbps(40), 0)
	ev("panel: 10 -> 40 Mbps")
	time.Sleep(7 * time.Second)
	setUser(uid, mbps(5), mbps(5), 0)
	ev("panel: 40 -> 5 Mbps")
	time.Sleep(7 * time.Second)
	setUser(uid, 0, 0, 0)
	ev("panel: 5 -> unlimited")
	time.Sleep(7 * time.Second)
	setUser(uid, mbps(10), mbps(10), 0)
	ev("panel: unlimited -> 10 Mbps")
	time.Sleep(7 * time.Second)
	close(stopc)
	wg.Wait()
	s.stop()
	ps := s.perSecond([]int{0})
	for i := range ps {
		ps[i] = r1(ps[i])
	}
	results["live_change"] = map[string]any{"events": events, "per_second_mbps": ps}
	logf("live per-second Mbps: %v", ps)
}

func scDevice() {
	const uid = 600 // device_limit 2
	res := map[string]any{}
	try := func(ip string) (bool, time.Duration) {
		t := time.Now()
		f := download("dev", uid, ip, 0)
		ok := acceptedWithin(f, 8*time.Second)
		el := time.Since(t)
		if ok {
			// make sure the connection really carries data, then keep it
			time.Sleep(300 * time.Millisecond)
			ok = f.ctr.Load() > 0 && !f.closed.Load()
		}
		f.stop()
		return ok, el
	}
	c0 := P.claims.Load()
	a, _ := try("127.0.0.2")
	b, _ := try("127.0.0.3")
	c, _ := try("127.0.0.4")
	res["limit2_dev1"], res["limit2_dev2"], res["limit2_dev3"] = a, b, c
	logf("device_limit=2: dev1 %v dev2 %v dev3 %v", a, b, c)
	// repeated connections from an admitted device within one cycle
	cBefore := P.claims.Load()
	for i := 0; i < 5; i++ {
		try("127.0.0.2")
	}
	res["claims_first_3_devices"] = P.claims.Load() - c0 - (P.claims.Load() - cBefore)
	res["claims_for_5_more_conns_same_cycle"] = P.claims.Load() - cBefore
	logf("claims for 5 more conns from an admitted device: %d (several may span a report cycle)", P.claims.Load()-cBefore)

	// panel claim endpoint failing: fail closed
	P.mu.Lock()
	P.claimMode = "error"
	P.mu.Unlock()
	time.Sleep(time.Duration(pushSec+1) * time.Second) // let the report cycle reset admitted ips
	okErrNew, _ := try("127.0.0.5")
	okErrOld, _ := try("127.0.0.2")
	res["panel_error_new_device"], res["panel_error_admitted_device_next_cycle"] = okErrNew, okErrOld
	logf("claim endpoint 500: new device %v, previously admitted device (next cycle) %v", okErrNew, okErrOld)

	P.mu.Lock()
	P.claimMode = "slow"
	P.mu.Unlock()
	okSlow, el := try("127.0.0.6")
	res["panel_slow_new_device"], res["panel_slow_reject_after_sec"] = okSlow, r1(el.Seconds())
	logf("claim endpoint hangs 6s: new device %v, decided after %.1fs", okSlow, el.Seconds())

	P.mu.Lock()
	P.claimMode = "ok"
	P.mu.Unlock()
	time.Sleep(time.Duration(pushSec+1) * time.Second)
	okBack, _ := try("127.0.0.2")
	res["panel_recovered_admitted_device"] = okBack
	logf("claim endpoint back: admitted device %v", okBack)

	// open connections are not cut when a 3rd device is rejected
	f1 := download("dev", uid, "127.0.0.2", 0)
	acceptedWithin(f1, 5*time.Second)
	try("127.0.0.4")
	time.Sleep(time.Second)
	res["open_conn_survives_rejection"] = !f1.closed.Load()
	f1.stop()
	results["device_limit"] = res
}

func scKick() {
	const uid = 700
	fa := download("a", uid, "127.0.0.2", 0)
	fb := download("b", uid, "127.0.0.3", 0)
	acceptedWithin(fa, 5*time.Second)
	acceptedWithin(fb, 5*time.Second)
	time.Sleep(time.Second)
	t0 := time.Now()
	P.mu.Lock()
	P.kicks = map[int]map[string]int{uid: {"127.0.0.2": 8}}
	P.mu.Unlock()
	for !fa.closed.Load() && time.Since(t0) < 10*time.Second {
		time.Sleep(20 * time.Millisecond)
	}
	closeAfter := time.Since(t0).Seconds()
	res := map[string]any{"kicked_conn_closed": fa.closed.Load(), "closed_after_sec": r1(closeAfter), "other_device_alive": !fb.closed.Load()}
	f := download("a2", uid, "127.0.0.2", 0)
	res["reconnect_during_block"] = acceptedWithin(f, 3*time.Second) && f.ctr.Load() >= 0 && !waitClosed(f, 500*time.Millisecond)
	f.stop()
	// the block ends 8 s after it was applied; expired blocks are cleared on the next pull
	for time.Since(t0) < 8*time.Second+time.Duration(pullSec+1)*time.Second {
		time.Sleep(100 * time.Millisecond)
	}
	f = download("a3", uid, "127.0.0.2", 0)
	res["reconnect_after_block"] = acceptedWithin(f, 5*time.Second)
	f.stop()
	fb.stop()
	logf("kick: %v", res)
	results["kick"] = res
}

func waitClosed(f *flow, d time.Duration) bool {
	t := time.Now()
	for time.Since(t) < d {
		if f.closed.Load() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return f.closed.Load()
}

// scOverhead compares the node's CPU with 200 users capped at 20 Mbps each
// against 200 unlimited users whose clients read at 20 Mbps themselves.
func scOverhead() {
	res := map[string]any{}
	run := func(name string, base int, pace int64) {
		var fs []*flow
		for i := 0; i < 200; i++ {
			fs = append(fs, download(name, base+i, "127.0.0.1", pace))
		}
		for _, f := range fs {
			acceptedWithin(f, 5*time.Second)
		}
		time.Sleep(2 * time.Second)
		c0, t0 := procCPU(nodePid), time.Now()
		go fetch(fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/profile?seconds=10", pprofPort), "cpu_"+name+".pb.gz")
		s := runFor(12*time.Second, fs...)
		cpu := float64(procCPU(nodePid)-c0) / 100 / time.Since(t0).Seconds() * 100
		rss := procRSS(nodePid)
		tot := s.stats(nil, 0)
		var per []float64
		for i := range fs {
			per = append(per, s.stats([]int{i}, 0).AvgMbps)
		}
		sort.Float64s(per)
		r := map[string]float64{"total_mbps": r1(tot.AvgMbps), "cpu_pct": r1(cpu), "rss_mib": r1(rss),
			"cpu_pct_per_gbps": r1(cpu / (tot.AvgMbps / 1000)), "per_user_min": r1(per[0]), "per_user_p50": r1(per[len(per)/2]), "per_user_max": r1(per[len(per)-1])}
		res[name] = r
		logf("overhead %-9s: total %.0f Mbps, CPU %.1f%% (%.1f%%/Gbps), RSS %.0f MiB, per user min/p50/max %.2f/%.2f/%.2f",
			name, tot.AvgMbps, cpu, r["cpu_pct_per_gbps"], rss, per[0], per[len(per)/2], per[len(per)-1])
		time.Sleep(time.Second)
	}
	run("limited", 1000, 0)
	run("baseline", 2000, mbps(20))
	results["overhead"] = res
}

func main() {
	flag.Parse()
	os.MkdirAll(*out, 0755)
	// users per scenario
	for i, m := range []float64{1, 5, 10, 50, 100, 500, 1000} {
		setUser(100+i, mbps(m), -1, 0)
	}
	setUser(200, mbps(20), mbps(5), 0)
	setLegacyUser(201, 8)
	setUser(202, mbps(10), 0, 0)
	setUser(300, mbps(20), -1, 0)
	setUser(301, mbps(20), -1, 0)
	for i := 0; i < 10; i++ {
		setUser(400+i, mbps(10), -1, 0)
	}
	setUser(410, 0, -1, 0)
	setUser(500, mbps(10), mbps(10), 0)
	setUser(600, 0, -1, 2)
	setUser(700, 0, -1, 0)
	for i := 0; i < 200; i++ {
		setUser(1000+i, mbps(20), -1, 0)
		setUser(2000+i, 0, -1, 0)
	}
	P.serve()
	targets()
	udpTargets()
	cfgPath, _ := filepath.Abs(filepath.Join(*out, "config.json"))
	os.WriteFile(cfgPath, []byte(fmt.Sprintf(`{"Log":{"Level":"warning","Access":"none"},"PprofPort":%d,"Nodes":[{"ApiHost":"http://127.0.0.1:%d","NodeID":1,"ApiKey":"k","Timeout":30}]}`, pprofPort, panelPort)), 0644)
	cmd := exec.Command("taskset", "-c", *cpus, *bin, "server", "-c", cfgPath, "-w=false")
	logFile, _ := os.Create(filepath.Join(*out, "v2node.log"))
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	defer cmd.Process.Kill()
	nodePid = cmd.Process.Pid
	if *proto != "vless" {
		startXrayClient()
		defer stopXrayClient()
	}
	for {
		f := download("ready", 410, "127.0.0.1", 0)
		if acceptedWithin(f, 2*time.Second) {
			f.stop()
			break
		}
		f.stop()
		time.Sleep(200 * time.Millisecond)
	}
	scenarios := []struct {
		name string
		fn   func()
	}{
		{"accuracy", scAccuracy}, {"directions", scDirections}, {"shared", scShared}, {"isolation", scIsolation},
		{"live", scLive}, {"device", scDevice}, {"kick", scKick}, {"overhead", scOverhead}, {"udp", scUDP},
	}
	for _, sc := range scenarios {
		if *only != "" && !strings.Contains(","+*only+",", ","+sc.name+",") {
			continue
		}
		logf("=== %s", sc.name)
		sc.fn()
	}
	b, _ := json.MarshalIndent(results, "", " ")
	os.WriteFile(filepath.Join(*out, "result.json"), b, 0644)
	os.Remove(cfgPath)
}
