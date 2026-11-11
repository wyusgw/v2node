package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	proto   = flag.String("proto", "vless", "node protocol: vless, hysteria2 or tuic")
	xrayBin = flag.String("xray", "", "xray binary used as the hysteria2/tuic client")
)

const (
	udpSrcPort    = 29011 // on a "go" packet, sends packets back at the asked rate
	udpSinkPort   = 29012 // counts packets by the 8-byte id they start with
	socksBasePort = 31000
	udpPayload    = 1200
)

// ---------------------------------------------------------------- tcp through the node

// openTCP opens a TCP stream to 127.0.0.1:port through the node as uid and
// writes first. With vless the node's response header still has to be read
// (vlessResp); through the socks client it doesn't exist.
func openTCP(uid int, srcIP string, port int, first []byte) (c net.Conn, vlessResp bool, err error) {
	if *proto == "vless" {
		c, err = dialFrom(srcIP)
		if err != nil {
			return nil, false, err
		}
		_, err = c.Write(append(vlessHeader(uid, port), first...))
		return c, true, err
	}
	c, err = socksConnect(uid, port)
	if err != nil {
		return nil, false, err
	}
	_, err = c.Write(first)
	return c, false, err
}

// ---------------------------------------------------------------- xray client

var (
	socksPort = map[int]int{}
	xrayCmd   *exec.Cmd
)

// startXrayClient runs one xray client with a socks inbound per user, each
// routed to its own hysteria2/tuic outbound authenticating as that user.
func startXrayClient() {
	P.mu.Lock()
	var uids []int
	for id := range P.users {
		if id < 1000 { // the overhead scenario's users are vless only
			uids = append(uids, id)
		}
	}
	P.mu.Unlock()
	sort.Ints(uids)
	var inbounds, outbounds, rules []any
	for i, uid := range uids {
		port := socksBasePort + i
		socksPort[uid] = port
		uuid := uuidStr(uuidOf(uid))
		in, out := fmt.Sprintf("in%d", uid), fmt.Sprintf("out%d", uid)
		inbounds = append(inbounds, map[string]any{"tag": in, "listen": "127.0.0.1", "port": port, "protocol": "socks",
			"settings": map[string]any{"auth": "noauth", "udp": true, "ip": "127.0.0.1"}})
		tlsS := map[string]any{"serverName": "test.local", "allowInsecure": true, "alpn": []string{"h3"}}
		var ob map[string]any
		switch *proto {
		case "hysteria2":
			ob = map[string]any{"tag": out, "protocol": "hysteria2",
				"settings": map[string]any{"version": 2, "address": "127.0.0.1", "port": nodePort},
				"streamSettings": map[string]any{"network": "hysteria2", "security": "tls", "tlsSettings": tlsS,
					"hysteria2Settings": map[string]any{"version": 2, "auth": uuid}}}
		case "tuic":
			ob = map[string]any{"tag": out, "protocol": "tuic",
				"settings":       map[string]any{"address": "127.0.0.1", "port": nodePort, "id": uuid, "password": uuid, "congestionControl": "bbr"},
				"streamSettings": map[string]any{"network": "tuic", "security": "tls", "tlsSettings": tlsS}}
		default:
			panic("unsupported -proto " + *proto)
		}
		outbounds = append(outbounds, ob)
		rules = append(rules, map[string]any{"inboundTag": []string{in}, "outboundTag": out})
	}
	cfg, _ := json.MarshalIndent(map[string]any{"log": map[string]any{"loglevel": "warning"},
		"inbounds": inbounds, "outbounds": outbounds, "routing": map[string]any{"rules": rules}}, "", " ")
	path, _ := filepath.Abs(filepath.Join(*out, "xray-client.json"))
	os.WriteFile(path, cfg, 0644)
	xrayCmd = exec.Command(*xrayBin, "run", "-c", path)
	lf, _ := os.Create(filepath.Join(*out, "xray-client.log"))
	xrayCmd.Stdout, xrayCmd.Stderr = lf, lf
	if err := xrayCmd.Start(); err != nil {
		panic(err)
	}
	for _, uid := range uids { // wait for the socks ports
		for {
			c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort[uid]))
			if err == nil {
				c.Close()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func stopXrayClient() {
	if xrayCmd != nil && xrayCmd.Process != nil {
		xrayCmd.Process.Kill()
	}
}

func socksHandshake(uid int, cmd byte, port int) (net.Conn, *net.UDPAddr, error) {
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort[uid]), 5*time.Second)
	if err != nil {
		return nil, nil, err
	}
	c.SetDeadline(time.Now().Add(8 * time.Second))
	defer c.SetDeadline(time.Time{})
	if _, err := c.Write([]byte{5, 1, 0}); err != nil {
		c.Close()
		return nil, nil, err
	}
	var b [10]byte
	if _, err := io.ReadFull(c, b[:2]); err != nil {
		c.Close()
		return nil, nil, err
	}
	req := []byte{5, cmd, 0, 1, 127, 0, 0, 1}
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	if cmd == 3 {
		req = []byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0}
	}
	if _, err := c.Write(req); err != nil {
		c.Close()
		return nil, nil, err
	}
	if _, err := io.ReadFull(c, b[:10]); err != nil || b[1] != 0 {
		c.Close()
		return nil, nil, fmt.Errorf("socks reply %v %v", b[1], err)
	}
	relay := &net.UDPAddr{IP: net.IPv4(b[4], b[5], b[6], b[7]), Port: int(binary.BigEndian.Uint16(b[8:10]))}
	return c, relay, nil
}

func socksConnect(uid, port int) (net.Conn, error) {
	c, _, err := socksHandshake(uid, 1, port)
	return c, err
}

// ---------------------------------------------------------------- udp targets

var udpSinkCounters sync.Map

func udpTargets() {
	src, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: udpSrcPort})
	if err != nil {
		panic(err)
	}
	go func() {
		var mu sync.Mutex
		active := map[string]bool{}
		buf := make([]byte, 2048)
		for {
			n, addr, err := src.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < 18 || string(buf[:2]) != "go" {
				continue
			}
			rate := int64(binary.BigEndian.Uint64(buf[2:10]))
			secs := int(binary.BigEndian.Uint64(buf[10:18]))
			mu.Lock()
			if active[addr.String()] {
				mu.Unlock()
				continue
			}
			active[addr.String()] = true
			mu.Unlock()
			go func(addr *net.UDPAddr) {
				pkt := make([]byte, udpPayload)
				end := time.Now().Add(time.Duration(secs) * time.Second)
				start := time.Now()
				var sent int64
				for time.Now().Before(end) {
					if ahead := time.Duration(float64(sent)/float64(rate)*1e9) - time.Since(start); ahead > 0 {
						time.Sleep(ahead)
					}
					src.WriteToUDP(pkt, addr)
					sent += udpPayload
				}
			}(addr)
		}
	}()
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: udpSinkPort})
	if err != nil {
		panic(err)
	}
	sink.SetReadBuffer(8 << 20)
	go func() {
		buf := make([]byte, 65536)
		for {
			n, _, err := sink.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < 8 {
				continue
			}
			v, _ := udpSinkCounters.LoadOrStore(binary.BigEndian.Uint64(buf[:8]), new(atomic.Int64))
			v.(*atomic.Int64).Add(int64(n))
		}
	}()
}

func goPacket(rate int64, secs int) []byte {
	p := []byte("go")
	p = binary.BigEndian.AppendUint64(p, uint64(rate))
	return binary.BigEndian.AppendUint64(p, uint64(secs))
}

// ---------------------------------------------------------------- udp clients

// udpConn sends and receives UDP payloads to 127.0.0.1:port through the node.
type udpConn interface {
	Send(p []byte) error
	Recv(buf []byte) (int, error)
	Close() error
}

// vlessUDP is VLESS command 2: one length-prefixed packet stream over TCP.
type vlessUDP struct {
	c       net.Conn
	r       *bufio.Reader
	gotResp bool
	wmu     sync.Mutex
}

func (v *vlessUDP) Send(p []byte) error {
	v.wmu.Lock()
	defer v.wmu.Unlock()
	b := binary.BigEndian.AppendUint16(nil, uint16(len(p)))
	_, err := v.c.Write(append(b, p...))
	return err
}

func (v *vlessUDP) Recv(buf []byte) (int, error) {
	if !v.gotResp {
		var h [2]byte
		if _, err := io.ReadFull(v.r, h[:]); err != nil {
			return 0, err
		}
		v.r.Discard(int(h[1]))
		v.gotResp = true
	}
	var l [2]byte
	if _, err := io.ReadFull(v.r, l[:]); err != nil {
		return 0, err
	}
	n := int(binary.BigEndian.Uint16(l[:]))
	if n > len(buf) {
		v.r.Discard(n)
		return n, nil
	}
	return io.ReadFull(v.r, buf[:n])
}

func (v *vlessUDP) Close() error { return v.c.Close() }

// socksUDP is a socks5 UDP association through the xray client.
type socksUDP struct {
	ctrl  net.Conn
	u     *net.UDPConn
	relay *net.UDPAddr
	hdr   []byte
}

func (s *socksUDP) Send(p []byte) error {
	_, err := s.u.WriteToUDP(append(append([]byte{}, s.hdr...), p...), s.relay)
	return err
}

func (s *socksUDP) Recv(buf []byte) (int, error) {
	tmp := make([]byte, 65536)
	n, _, err := s.u.ReadFromUDP(tmp)
	if err != nil {
		return 0, err
	}
	if n < 10 {
		return 0, nil
	}
	return copy(buf, tmp[10:n]), nil
}

func (s *socksUDP) Close() error { s.u.Close(); return s.ctrl.Close() }

func openUDP(uid, port int) (udpConn, error) {
	if *proto == "vless" {
		c, err := dialFrom("127.0.0.1")
		if err != nil {
			return nil, err
		}
		h := vlessHeader(uid, port)
		h[17+1] = 2 // command: UDP
		if _, err := c.Write(h); err != nil {
			return nil, err
		}
		return &vlessUDP{c: c, r: bufio.NewReaderSize(c, 256<<10)}, nil
	}
	ctrl, relay, err := socksHandshake(uid, 3, 0)
	if err != nil {
		return nil, err
	}
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		ctrl.Close()
		return nil, err
	}
	u.SetReadBuffer(8 << 20)
	if relay.IP.IsUnspecified() {
		relay.IP = net.IPv4(127, 0, 0, 1)
	}
	hdr := []byte{0, 0, 0, 1, 127, 0, 0, 1}
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(port))
	return &socksUDP{ctrl: ctrl, u: u, relay: relay, hdr: hdr}, nil
}

type closerConn struct{ udpConn }

// udpDownload asks the UDP source to send at srcRate for secs and counts what
// arrives through the node.
func udpDownload(uid int, srcRate int64, secs int) *flow {
	f := &flow{name: "udp-down", ctr: new(atomic.Int64), accepted: make(chan bool, 1)}
	u, err := openUDP(uid, udpSrcPort)
	if err != nil {
		setAccepted(f, false)
		markClosed(f)
		return f
	}
	f.extra = u
	go func() { // repeat the request until data flows (UDP can lose it)
		for i := 0; i < 20 && f.ctr.Load() == 0 && !f.closed.Load(); i++ {
			u.Send(goPacket(srcRate, secs))
			time.Sleep(200 * time.Millisecond)
		}
	}()
	go func() {
		defer markClosed(f)
		buf := make([]byte, 65536)
		for {
			n, err := u.Recv(buf)
			if err != nil {
				setAccepted(f, false)
				return
			}
			if n > 0 {
				setAccepted(f, true)
			}
			f.ctr.Add(int64(n))
		}
	}()
	return f
}

// udpUpload sends to the UDP sink at rate; the sink counts what arrives.
func udpUpload(uid int, rate int64) *flow {
	id := flowID.Add(1)
	v, _ := udpSinkCounters.LoadOrStore(id, new(atomic.Int64))
	f := &flow{name: "udp-up", ctr: v.(*atomic.Int64), accepted: make(chan bool, 1)}
	u, err := openUDP(uid, udpSinkPort)
	if err != nil {
		setAccepted(f, false)
		markClosed(f)
		return f
	}
	f.extra = u
	go func() {
		for i := 0; i < 500 && f.ctr.Load() == 0 && !f.closed.Load(); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		setAccepted(f, f.ctr.Load() > 0)
	}()
	go func() {
		defer markClosed(f)
		pkt := make([]byte, udpPayload)
		binary.BigEndian.PutUint64(pkt, id)
		start := time.Now()
		var sent int64
		for !f.closed.Load() {
			if ahead := time.Duration(float64(sent)/float64(rate)*1e9) - time.Since(start); ahead > 0 {
				time.Sleep(ahead)
			}
			if err := u.Send(pkt); err != nil {
				return
			}
			sent += udpPayload
		}
	}()
	return f
}

func scUDP() {
	type row struct {
		Dir       string  `json:"dir"`
		LimitMbps float64 `json:"limit_mbps"`
		OfferMbps float64 `json:"offered_mbps"`
		AvgMbps   float64 `json:"avg_mbps"`
		P95Mbps   float64 `json:"p95_100ms_mbps"`
		ErrPct    float64 `json:"err_pct"`
	}
	var rows []row
	cases := []struct {
		uid   int
		limit float64
	}{{102, 10}, {103, 50}, {104, 100}}
	const offer = 300.0
	for _, c := range cases {
		for _, dir := range []string{"down", "up"} {
			var f *flow
			if dir == "down" {
				f = udpDownload(c.uid, mbps(offer), 11)
			} else {
				f = udpUpload(c.uid, mbps(offer))
			}
			if !acceptedWithin(f, 6*time.Second) {
				logf("udp %s %v: no data", dir, c.limit)
				f.stop()
				continue
			}
			s := runFor(8*time.Second, f)
			st := s.stats(nil, 2)
			r := row{dir, c.limit, offer, r1(st.AvgMbps), r1(st.P95Mbps), r1((st.AvgMbps/c.limit - 1) * 100)}
			rows = append(rows, r)
			logf("udp %-4s limit %4.0f Mbps (offered %v): avg %.2f (%+.1f%%) 100ms p95 %.1f", dir, c.limit, offer, st.AvgMbps, r.ErrPct, st.P95Mbps)
			time.Sleep(3 * time.Second) // let the source's blast end
		}
	}
	// an unlimited user, to show the node itself passes the offered rate
	for _, dir := range []string{"down", "up"} {
		var f *flow
		if dir == "down" {
			f = udpDownload(410, mbps(offer), 11)
		} else {
			f = udpUpload(410, mbps(offer))
		}
		if acceptedWithin(f, 6*time.Second) {
			s := runFor(8*time.Second, f)
			st := s.stats(nil, 2)
			rows = append(rows, row{dir, 0, offer, r1(st.AvgMbps), r1(st.P95Mbps), 0})
			logf("udp %-4s unlimited (offered %v): avg %.2f", dir, offer, st.AvgMbps)
		} else {
			f.stop()
			logf("udp %s unlimited: no data", dir)
		}
		time.Sleep(3 * time.Second)
	}
	results["udp"] = rows
}
