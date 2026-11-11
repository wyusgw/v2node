// bench: mock v2board panel + echo/source target + VLESS load generator,
// drives a v2node child process and samples its CPU/RSS and pprof.
package main

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	bin        = flag.String("bin", "", "v2node binary")
	out        = flag.String("out", "out", "output dir")
	users      = flag.Int("users", 1000, "users on panel")
	conns      = flag.Int("conns", 1000, "concurrent idle/active conns")
	cpus       = flag.String("cpus", "0", "taskset cpu list for v2node")
	gomaxprocs = flag.Int("procs", 1, "GOMAXPROCS for v2node")
	memlimit   = flag.String("memlimit", "", "GOMEMLIMIT for v2node")
	bulk       = flag.Int("bulk", 16, "bulk download conns")
	holdSec    = flag.Int("hold", 30, "active phase seconds")
	bulkSec    = flag.Int("bulksec", 15, "bulk phase seconds")
	chunk      = flag.Int("chunk", 4096, "bytes echoed per active tick")
)

const panelPort, nodePort, echoPort, srcPort, pprofPort = 18080, 18443, 19000, 19001, 16060

func uuidOf(i int) [16]byte {
	h := md5.Sum([]byte("user-" + strconv.Itoa(i)))
	h[6] = h[6]&0x0f | 0x40
	h[8] = h[8]&0x3f | 0x80
	return h
}
func uuidStr(u [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

func panel() {
	type U struct {
		Id          int    `json:"id"`
		Uuid        string `json:"uuid"`
		SpeedLimit  int    `json:"speed_limit"`
		DeviceLimit int    `json:"device_limit"`
	}
	ul := make([]U, *users)
	for i := range ul {
		ul[i] = U{Id: i + 1, Uuid: uuidStr(uuidOf(i + 1))}
	}
	userBody, _ := json.Marshal(map[string]any{"users": ul})
	// freedom blocks private targets for vless by default; route the bench
	// inbound to a freedom outbound whose finalRules allow loopback.
	ob, _ := json.Marshal(map[string]any{"tag": "benchout", "protocol": "freedom",
		"settings": map[string]any{"finalRules": []any{map[string]any{"action": "allow", "ip": []string{"127.0.0.1/32"}}}}})
	cm, _ := json.Marshal(map[string]any{"protocol": "vless", "listen_ip": "127.0.0.1", "server_port": nodePort, "network": "tcp", "tls": 0,
		"routes":      []any{map[string]any{"id": 1, "match": []string{}, "action": "default_out", "action_value": string(ob)}},
		"base_config": map[string]any{"push_interval": 60, "pull_interval": 60}})
	cfg := string(cm)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/server/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "cfg1")
		if r.Header.Get("If-None-Match") == "cfg1" {
			w.WriteHeader(304)
			return
		}
		io.WriteString(w, cfg)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/user", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "u1")
		if r.Header.Get("If-None-Match") == "u1" {
			w.WriteHeader(304)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(userBody)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/alivelist", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"alive":{}}`) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"data":true}`)
	})
	go http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", panelPort), mux)
}

func targets() {
	le, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", echoPort))
	go func() {
		for {
			c, err := le.Accept()
			if err != nil {
				return
			}
			go func() {
				// plain buffer loop: io.Copy between TCPConns splices through a
				// pipe pair per conn, doubling the harness's fd use
				defer c.Close()
				b := make([]byte, 8192)
				for {
					n, err := c.Read(b)
					if n > 0 {
						if _, werr := c.Write(b[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	ls, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", srcPort))
	go func() {
		buf := make([]byte, 64<<10)
		for {
			c, err := ls.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					if _, err := c.Write(buf); err != nil {
						return
					}
				}
			}()
		}
	}()
}

// dialVLESS opens a VLESS (no flow, no encryption) TCP tunnel to 127.0.0.1:port.
func dialVLESS(uid, port int) (net.Conn, *bufio.Reader, error) {
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", nodePort), 5*time.Second)
	if err != nil {
		return nil, nil, err
	}
	u := uuidOf(uid)
	h := []byte{0}
	h = append(h, u[:]...)
	h = append(h, 0, 1)
	h = binary.BigEndian.AppendUint16(h, uint16(port))
	h = append(h, 1, 127, 0, 0, 1)
	if _, err := c.Write(h); err != nil {
		c.Close()
		return nil, nil, err
	}
	return c, bufio.NewReaderSize(c, 32<<10), nil
}

func readResp(r *bufio.Reader) error {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	_, err := r.Discard(int(b[1]))
	return err
}

type sample struct {
	T      float64 `json:"t"`
	Phase  string  `json:"phase"`
	CPU    float64 `json:"cpu_pct"`
	RSSMiB float64 `json:"rss_mib"`
}

var (
	samples   []sample
	smu       sync.Mutex
	phase     atomic.Value
	echoOps   atomic.Int64
	bulkBytes atomic.Int64
	connErr   atomic.Int64
	errKinds  sync.Map
)

func noteErr(stage string, err error) {
	connErr.Add(1)
	m := stage
	if err != nil {
		e := err.Error()
		if i := strings.LastIndex(e, ": "); i >= 0 {
			e = e[i+2:]
		}
		m += ": " + e
	}
	v, _ := errKinds.LoadOrStore(m, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func procStat(pid int) (ticks int64, rssKB int64) {
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	s := string(b)
	if i := strings.LastIndex(s, ")"); i > 0 {
		f := strings.Fields(s[i+2:])
		if len(f) > 13 {
			u, _ := strconv.ParseInt(f[11], 10, 64)
			k, _ := strconv.ParseInt(f[12], 10, 64)
			ticks = u + k
		}
	}
	st, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	for _, l := range strings.Split(string(st), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			rssKB, _ = strconv.ParseInt(strings.Fields(l)[1], 10, 64)
		}
	}
	return
}

func sampler(pid int, start time.Time, stop chan struct{}) {
	lt, _ := procStat(pid)
	lw := time.Now()
	tk := time.NewTicker(time.Second)
	for {
		select {
		case <-stop:
			return
		case now := <-tk.C:
			t, rss := procStat(pid)
			cpu := float64(t-lt) / 100 / now.Sub(lw).Seconds() * 100
			lt, lw = t, now
			smu.Lock()
			samples = append(samples, sample{now.Sub(start).Seconds(), phase.Load().(string), cpu, float64(rss) / 1024})
			smu.Unlock()
		}
	}
}

func fetch(url, file string) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("fetch", url, err)
		return
	}
	defer resp.Body.Close()
	f, _ := os.Create(filepath.Join(*out, file))
	io.Copy(f, resp.Body)
	f.Close()
}

func pp(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/%s", pprofPort, path)
}

func phaseStats(p string) (avgCPU, maxCPU, avgRSS, maxRSS float64) {
	smu.Lock()
	defer smu.Unlock()
	n := 0
	for _, s := range samples {
		if s.Phase != p {
			continue
		}
		n++
		avgCPU += s.CPU
		avgRSS += s.RSSMiB
		if s.CPU > maxCPU {
			maxCPU = s.CPU
		}
		if s.RSSMiB > maxRSS {
			maxRSS = s.RSSMiB
		}
	}
	if n > 0 {
		avgCPU /= float64(n)
		avgRSS /= float64(n)
	}
	return
}

func main() {
	flag.Parse()
	os.MkdirAll(*out, 0755)
	panel()
	targets()
	cfgPath := filepath.Join(*out, "config.json")
	os.WriteFile(cfgPath, []byte(fmt.Sprintf(`{"Log":{"Level":"warning","Access":"none"},"PprofPort":%d,"Nodes":[{"ApiHost":"http://127.0.0.1:%d","NodeID":1,"ApiKey":"k","Timeout":30}]}`, pprofPort, panelPort)), 0644)
	cmd := exec.Command("taskset", "-c", *cpus, *bin, "server", "-c", cfgPath, "-w=false")
	env := append(os.Environ(), "GOMAXPROCS="+strconv.Itoa(*gomaxprocs))
	if *memlimit != "" {
		env = append(env, "GOMEMLIMIT="+*memlimit)
	}
	cmd.Env = env
	logf, _ := os.Create(filepath.Join(*out, "v2node.log"))
	cmd.Stdout, cmd.Stderr = logf, logf
	start := time.Now()
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	defer cmd.Process.Kill()
	pid := cmd.Process.Pid
	phase.Store("startup")
	stop := make(chan struct{})
	go sampler(pid, start, stop)

	// wait until node accepts and a VLESS handshake for the last user succeeds
	var ready time.Duration
	for {
		c, r, err := dialVLESS(*users, echoPort)
		if err == nil {
			c.Write([]byte("x"))
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			if readResp(r) == nil {
				c.Close()
				ready = time.Since(start)
				break
			}
			c.Close()
		}
		if time.Since(start) > 120*time.Second {
			panic("node not ready")
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("ready after %v\n", ready)

	phase.Store("idle")
	time.Sleep(10 * time.Second)
	fetch(pp("heap?gc=1"), "heap_idle.pb.gz")

	// active phase: conns connections, each echoes chunk bytes once per second
	phase.Store("ramp")
	var wg sync.WaitGroup
	done := make(chan struct{})
	var opened atomic.Int64
	rampStart := time.Now()
	sem := make(chan struct{}, 64)
	for i := 0; i < *conns; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uid := i%*users + 1
			c, r, err := dialVLESS(uid, echoPort)
			<-sem
			if err != nil {
				noteErr("dial", err)
				return
			}
			defer c.Close()
			buf := make([]byte, *chunk)
			first := true
			opened.Add(1)
			// jitter start so ticks are spread over the second
			time.Sleep(time.Duration(i%1000) * time.Millisecond)
			tk := time.NewTicker(time.Second)
			defer tk.Stop()
			for {
				c.SetDeadline(time.Now().Add(20 * time.Second))
				if _, err := c.Write(buf); err != nil {
					noteErr("write", err)
					return
				}
				if first {
					if err := readResp(r); err != nil {
						noteErr("resp", err)
						return
					}
					first = false
				}
				if _, err := io.ReadFull(r, buf); err != nil {
					noteErr("read", err)
					return
				}
				echoOps.Add(1)
				select {
				case <-done:
					return
				case <-tk.C:
				}
			}
		}(i)
	}
	for len(sem) > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	rampDur := time.Since(rampStart)
	time.Sleep(3 * time.Second)
	phase.Store("active")
	ops0 := echoOps.Load()
	t0 := time.Now()
	go fetch(pp(fmt.Sprintf("profile?seconds=%d", *holdSec-5)), "cpu_active.pb.gz")
	time.Sleep(time.Duration(*holdSec) * time.Second)
	activeOps := float64(echoOps.Load()-ops0) / time.Since(t0).Seconds()
	fetch(pp("heap"), "heap_active.pb.gz")
	fetch(pp("goroutine"), "goroutine_active.pb.gz")
	gr := 0
	if resp, err := http.Get(pp("goroutine?debug=1")); err == nil {
		l, _ := bufio.NewReader(resp.Body).ReadString('\n')
		fmt.Sscanf(l, "goroutine profile: total %d", &gr)
		resp.Body.Close()
	}

	// bulk phase on top of the active connections
	phase.Store("bulk")
	bdone := make(chan struct{})
	var bwg sync.WaitGroup
	for i := 0; i < *bulk; i++ {
		bwg.Add(1)
		go func(i int) {
			defer bwg.Done()
			c, r, err := dialVLESS(i%*users+1, srcPort)
			if err != nil {
				return
			}
			defer c.Close()
			c.Write([]byte{0})
			if readResp(r) != nil {
				return
			}
			buf := make([]byte, 64<<10)
			go func() { <-bdone; c.Close() }()
			for {
				n, err := r.Read(buf)
				bulkBytes.Add(int64(n))
				if err != nil {
					return
				}
			}
		}(i)
	}
	time.Sleep(2 * time.Second)
	b0 := bulkBytes.Load()
	tb := time.Now()
	go fetch(pp(fmt.Sprintf("profile?seconds=%d", *bulkSec-3)), "cpu_bulk.pb.gz")
	time.Sleep(time.Duration(*bulkSec) * time.Second)
	gbps := float64(bulkBytes.Load()-b0) * 8 / time.Since(tb).Seconds() / 1e9
	fetch(pp("heap"), "heap_bulk.pb.gz")
	close(bdone)
	bwg.Wait()
	close(done)
	wg.Wait()
	phase.Store("drain")
	time.Sleep(8 * time.Second)
	fetch(pp("heap?gc=1"), "heap_after.pb.gz")
	close(stop)

	res := map[string]any{"users": *users, "conns": *conns, "opened": opened.Load(), "conn_errors": connErr.Load(),
		"cpus": *cpus, "gomaxprocs": *gomaxprocs, "memlimit": *memlimit, "ready_sec": ready.Seconds(),
		"ramp_sec": rampDur.Seconds(), "echo_ops_per_sec": activeOps, "goroutines_active": gr, "bulk_gbps": gbps,
		"host_cpus": runtime.NumCPU()}
	for _, p := range []string{"startup", "idle", "ramp", "active", "bulk", "drain"} {
		a, m, r, mr := phaseStats(p)
		res[p] = map[string]float64{"cpu_avg": a, "cpu_max": m, "rss_avg": r, "rss_max": mr}
	}
	smu.Lock()
	res["samples"] = samples
	smu.Unlock()
	ek := map[string]int64{}
	errKinds.Range(func(k, v any) bool { ek[k.(string)] = v.(*atomic.Int64).Load(); return true })
	res["error_kinds"] = ek
	fmt.Println("error kinds:", ek)
	b, _ := json.MarshalIndent(res, "", " ")
	os.WriteFile(filepath.Join(*out, "result.json"), b, 0644)
	fmt.Printf("users=%d conns=%d opened=%d errs=%d ready=%.1fs echo/s=%.0f gr=%d bulk=%.2fGbps\n", *users, *conns, opened.Load(), connErr.Load(), ready.Seconds(), activeOps, gr, gbps)
	for _, p := range []string{"idle", "active", "bulk", "drain"} {
		a, m, r, mr := phaseStats(p)
		fmt.Printf("  %-7s cpu avg %.1f%% max %.1f%%  rss avg %.1f max %.1f MiB\n", p, a, m, r, mr)
	}
}
