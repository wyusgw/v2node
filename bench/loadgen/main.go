// Command loadgen drives many concurrent proxied TCP connections, spread
// over many users, through a v2node inbound and into a local traffic sink,
// so the node's CPU and memory can be measured at a fixed, known load.
//
// Every connection authenticates as a random benchmark user
// (bench/internal/users), asks the sink for a fixed download rate and sends
// a fixed upload rate itself. With -life set, connections are closed and
// redialed (as a new random user) so handshakes keep happening as they do
// in production.
package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc64"
	"io"
	"log"
	"math/rand/v2"
	gonet "net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wyusgw/v2node/bench/internal/users"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/shadowsocks"
	"github.com/xtls/xray-core/proxy/trojan"
	"github.com/xtls/xray-core/proxy/vless"
	vlessenc "github.com/xtls/xray-core/proxy/vless/encoding"
	"github.com/xtls/xray-core/proxy/vmess"
	vmessenc "github.com/xtls/xray-core/proxy/vmess/encoding"
)

var (
	proto    = flag.String("protocol", "vless", "vless | vmess | trojan | shadowsocks")
	server   = flag.String("server", "127.0.0.1:20443", "node address")
	sinkAddr = flag.String("sink", "127.0.0.1:19000", "sink listen address")
	target   = flag.String("target", "", "address the node is asked to connect to (default: -sink); must be public, since xray's freedom outbound refuses private targets")
	nUsers   = flag.Int("users", 1000, "users to pick from (must match mockpanel)")
	conns    = flag.Int("conns", 100, "concurrent connections")
	down     = flag.Int("down", 32<<10, "per-connection download rate, bytes/s")
	up       = flag.Int("up", 4<<10, "per-connection upload rate, bytes/s")
	life     = flag.Duration("life", 20*time.Second, "mean connection lifetime (0 = keep for the whole run)")
	ramp     = flag.Duration("ramp", 10*time.Second, "time over which connections are opened")
	duration = flag.Duration("duration", 40*time.Second, "total run time including ramp")
	cipher   = flag.String("cipher", "aes-128-gcm", "shadowsocks cipher")
	out      = flag.String("out", "", "write the JSON summary here as well as stdout")
	sinkOnly = flag.Bool("sink-only", false, "only run the sink")
)

const tick = 100 * time.Millisecond

var (
	bytesDown, bytesUp     atomic.Int64
	dialOK, dialErr, ioErr atomic.Int64
	active                 atomic.Int64
	latMu                  sync.Mutex
	latencies              []time.Duration
	dialer                 = gonet.Dialer{Timeout: 10 * time.Second}
)

func main() {
	flag.Parse()
	go runSink(*sinkAddr)
	if *sinkOnly {
		select {}
	}
	time.Sleep(200 * time.Millisecond)

	if *target == "" {
		*target = *sinkAddr
	}
	sinkHost, sinkPort, _ := gonet.SplitHostPort(*target)
	dest := net.TCPDestination(net.ParseAddress(sinkHost), net.Port(mustAtoi(sinkPort)))

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	start := time.Now()
	// Throughput is reported for the steady window after the ramp only.
	var steadyD, steadyU atomic.Int64
	steadyAt := time.AfterFunc(*ramp, func() {
		steadyD.Store(bytesDown.Load())
		steadyU.Store(bytesUp.Load())
	})
	defer steadyAt.Stop()
	var wg sync.WaitGroup
	for i := 0; i < *conns; i++ {
		delay := time.Duration(int64(*ramp) * int64(i) / int64(max(*conns, 1)))
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
			worker(ctx, dest, rand.New(rand.NewPCG(seed, seed*7919)))
		}(uint64(i + 1))
	}

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		var lastD, lastU int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				d, u := bytesDown.Load(), bytesUp.Load()
				log.Printf("t=%3.0fs active=%d dials=%d errs=%d/%d down=%.1fMB/s up=%.1fMB/s",
					time.Since(start).Seconds(), active.Load(), dialOK.Load(), dialErr.Load(), ioErr.Load(),
					float64(d-lastD)/5/1e6, float64(u-lastU)/5/1e6)
				lastD, lastU = d, u
			}
		}
	}()
	wg.Wait()

	steady := time.Since(start).Seconds() - ramp.Seconds()
	latMu.Lock()
	slices.Sort(latencies)
	pct := func(p float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		return float64(latencies[int(p*float64(len(latencies)-1))].Microseconds()) / 1000
	}
	summary := map[string]any{
		"protocol":         *proto,
		"users":            *nUsers,
		"conns":            *conns,
		"target_down_MBps": float64(*down**conns) / 1e6,
		"target_up_MBps":   float64(*up**conns) / 1e6,
		"down_MBps":        float64(bytesDown.Load()-steadyD.Load()) / steady / 1e6,
		"up_MBps":          float64(bytesUp.Load()-steadyU.Load()) / steady / 1e6,
		"dials":            dialOK.Load(),
		"dial_errors":      dialErr.Load(),
		"io_errors":        ioErr.Load(),
		"ttfb_p50_ms":      pct(0.50),
		"ttfb_p99_ms":      pct(0.99),
	}
	latMu.Unlock()
	b, _ := json.Marshal(summary)
	fmt.Println(string(b))
	if *out != "" {
		os.WriteFile(*out, append(b, '\n'), 0o644)
	}
}

// worker keeps one connection slot busy until ctx ends, redialing as a new
// random user whenever the current connection's lifetime runs out.
func worker(ctx context.Context, dest net.Destination, r *rand.Rand) {
	for ctx.Err() == nil {
		lifetime := *duration
		if *life > 0 {
			lifetime = time.Duration(float64(*life) * (0.5 + r.Float64()))
		}
		cctx, cancel := context.WithTimeout(ctx, lifetime)
		if err := session(cctx, dest, r.IntN(*nUsers)); err != nil && ctx.Err() == nil {
			time.Sleep(200 * time.Millisecond)
		}
		cancel()
	}
}

type stream struct {
	conn  gonet.Conn
	write func([]byte) error
	read  func() (int, error)
}

func session(ctx context.Context, dest net.Destination, uidx int) error {
	conn, err := dialer.DialContext(ctx, "tcp", *server)
	if err != nil {
		dialErr.Add(1)
		return err
	}
	defer conn.Close()
	// An HTTP request line, like real clients' first packet, is recognised
	// by the node's sniffer at once; unrecognisable bytes would make it wait
	// for more data before dispatching.
	hello := fmt.Sprintf("GET /%d HTTP/1.1\r\nHost: %s\r\n\r\n", *down, dest.Address)
	begin := time.Now()
	s, err := open(conn, users.UUID(uidx), dest, []byte(hello))
	if err != nil {
		dialErr.Add(1)
		return err
	}
	dialOK.Add(1)
	active.Add(1)
	defer active.Add(-1)
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	done := make(chan error, 1)
	go func() {
		first := true
		for {
			n, err := s.read()
			if n > 0 {
				if first {
					first = false
					latMu.Lock()
					if len(latencies) < 1_000_000 {
						latencies = append(latencies, time.Since(begin))
					}
					latMu.Unlock()
				}
				bytesDown.Add(int64(n))
			}
			if err != nil {
				done <- err
				return
			}
		}
	}()

	chunk := make([]byte, *up/int(time.Second/tick))
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-done:
			if ctx.Err() == nil {
				ioErr.Add(1)
			}
			return err
		case <-t.C:
			if len(chunk) == 0 {
				continue
			}
			if err := s.write(chunk); err != nil {
				if ctx.Err() == nil {
					ioErr.Add(1)
				}
				return err
			}
			bytesUp.Add(int64(len(chunk)))
		}
	}
}

// open performs the client side of the protocol handshake on conn, sending
// first as the initial payload, the way real clients coalesce it with the
// request header.
func open(conn gonet.Conn, id string, dest net.Destination, first []byte) (*stream, error) {
	s := &stream{conn: conn}
	switch *proto {
	case "vless":
		acc, err := (&vless.Account{Id: id}).AsAccount()
		if err != nil {
			return nil, err
		}
		req := &protocol.RequestHeader{
			Version: 0, Command: protocol.RequestCommandTCP,
			Address: dest.Address, Port: dest.Port,
			User: &protocol.MemoryUser{Account: acc},
		}
		bw := buf.NewBufferedWriter(buf.NewWriter(conn))
		if err := vlessenc.EncodeRequestHeader(bw, req, &vlessenc.Addons{}); err != nil {
			return nil, err
		}
		bw.Write(first)
		if err := bw.SetBuffered(false); err != nil {
			return nil, err
		}
		br := bufio.NewReaderSize(conn, 32<<10)
		headerRead := false
		rbuf := make([]byte, 32<<10)
		s.write = func(p []byte) error { _, err := conn.Write(p); return err }
		s.read = func() (int, error) {
			if !headerRead {
				if _, err := vlessenc.DecodeResponseHeader(br, req); err != nil {
					return 0, err
				}
				headerRead = true
			}
			return br.Read(rbuf)
		}
	case "trojan":
		acc, err := (&trojan.Account{Password: id}).AsAccount()
		if err != nil {
			return nil, err
		}
		w := &trojan.ConnWriter{Writer: conn, Target: dest, Account: acc.(*trojan.MemoryAccount)}
		if _, err := w.Write(first); err != nil {
			return nil, err
		}
		rbuf := make([]byte, 32<<10)
		s.write = func(p []byte) error { _, err := conn.Write(p); return err }
		s.read = func() (int, error) { return conn.Read(rbuf) }
	case "vmess":
		acc, err := (&conf.VMessAccount{ID: id, Security: "aes-128-gcm"}).Build().AsAccount()
		if err != nil {
			return nil, err
		}
		ma := acc.(*vmess.MemoryAccount)
		req := &protocol.RequestHeader{
			Version: vmessenc.Version, Command: protocol.RequestCommandTCP,
			Address: dest.Address, Port: dest.Port,
			User:     &protocol.MemoryUser{Account: acc},
			Option:   protocol.RequestOptionChunkStream | protocol.RequestOptionChunkMasking | protocol.RequestOptionGlobalPadding,
			Security: ma.Security,
		}
		kdf := hmac.New(sha256.New, []byte("VMessBF"))
		kdf.Write(ma.ID.Bytes())
		cs := vmessenc.NewClientSession(context.Background(), int64(crc64.Checksum(kdf.Sum(nil), crc64.MakeTable(crc64.ISO))))
		bw := buf.NewBufferedWriter(buf.NewWriter(conn))
		if err := cs.EncodeRequestHeader(req, bw); err != nil {
			return nil, err
		}
		body, err := cs.EncodeRequestBody(req, bw)
		if err != nil {
			return nil, err
		}
		if err := body.WriteMultiBuffer(buf.MergeBytes(nil, first)); err != nil {
			return nil, err
		}
		if err := bw.SetBuffered(false); err != nil {
			return nil, err
		}
		s.write = func(p []byte) error { return body.WriteMultiBuffer(buf.MergeBytes(nil, p)) }
		s.read = readerFrom(func() (buf.Reader, error) {
			br := &buf.BufferedReader{Reader: buf.NewReader(conn)}
			if _, err := cs.DecodeResponseHeader(br); err != nil {
				return nil, err
			}
			return cs.DecodeResponseBody(req, br)
		})
	case "shadowsocks":
		ct := map[string]shadowsocks.CipherType{
			"aes-128-gcm":       shadowsocks.CipherType_AES_128_GCM,
			"aes-256-gcm":       shadowsocks.CipherType_AES_256_GCM,
			"chacha20-poly1305": shadowsocks.CipherType_CHACHA20_POLY1305,
		}[*cipher]
		acc, err := (&shadowsocks.Account{Password: id, CipherType: ct}).AsAccount()
		if err != nil {
			return nil, err
		}
		user := &protocol.MemoryUser{Account: acc}
		req := &protocol.RequestHeader{
			Version: shadowsocks.Version, Command: protocol.RequestCommandTCP,
			Address: dest.Address, Port: dest.Port, User: user,
		}
		bw := buf.NewBufferedWriter(buf.NewWriter(conn))
		body, err := shadowsocks.WriteTCPRequest(req, bw)
		if err != nil {
			return nil, err
		}
		if err := body.WriteMultiBuffer(buf.MergeBytes(nil, first)); err != nil {
			return nil, err
		}
		if err := bw.SetBuffered(false); err != nil {
			return nil, err
		}
		s.write = func(p []byte) error { return body.WriteMultiBuffer(buf.MergeBytes(nil, p)) }
		s.read = readerFrom(func() (buf.Reader, error) { return shadowsocks.ReadTCPResponse(user, conn) })
	default:
		return nil, fmt.Errorf("unknown protocol %q", *proto)
	}
	return s, nil
}

// readerFrom adapts a lazily opened buf.Reader (whose response header is
// only readable once the server has answered) to a byte-counting read.
func readerFrom(openReader func() (buf.Reader, error)) func() (int, error) {
	var r buf.Reader
	return func() (int, error) {
		if r == nil {
			var err error
			if r, err = openReader(); err != nil {
				return 0, err
			}
		}
		mb, err := r.ReadMultiBuffer()
		n := int(mb.Len())
		buf.ReleaseMulti(mb)
		return n, err
	}
}

// runSink accepts proxied connections. Each one starts with
// "GET /<bytes per second> HTTP/1.1" plus headers; the sink then sends at
// that rate and discards whatever the client uploads.
func runSink(addr string) {
	ln, err := gonet.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("sink: %v", err)
	}
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i * 131)
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go func(c gonet.Conn) {
			defer c.Close()
			br := bufio.NewReader(c)
			var rate int
			if _, err := fmt.Fscanf(br, "GET /%d HTTP/1.1\r\n", &rate); err != nil {
				return
			}
			for {
				line, err := br.ReadString('\n')
				if err != nil {
					return
				}
				if line == "\r\n" {
					break
				}
			}
			per := rate / int(time.Second/tick)
			if per <= 0 {
				io.Copy(io.Discard, br)
				return
			}
			go io.Copy(io.Discard, br)
			t := time.NewTicker(tick)
			defer t.Stop()
			for {
				for off := 0; off < per; {
					n := min(per-off, len(payload))
					if _, err := c.Write(payload[:n]); err != nil {
						return
					}
					off += n
				}
				<-t.C
			}
		}(c)
	}
}

func mustAtoi(s string) int {
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		log.Fatalf("bad port %q", s)
	}
	return n
}
