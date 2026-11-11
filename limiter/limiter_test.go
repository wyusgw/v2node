package limiter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	panel "github.com/wyusgw/v2node/api/v2board"
	"github.com/wyusgw/v2node/common/format"
	"github.com/wyusgw/v2node/conf"
)

const tag = "test-tag"

func i64(v int64) *int64 { return &v }

func TestDetermineSpeedLimit(t *testing.T) {
	cases := []struct{ a, b, want int64 }{
		{0, 0, 0},
		{0, 100, 100},
		{100, 0, 100},
		{100, 200, 100},
		{200, 100, 100},
		{150, 150, 150},
	}
	for _, c := range cases {
		if got := determineSpeedLimit(c.a, c.b); got != c.want {
			t.Errorf("determineSpeedLimit(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestUserInfoSpeedLimitFields(t *testing.T) {
	cases := []struct {
		name     string
		u        panel.UserInfo
		down, up int64
	}{
		{"legacy mbps only, applies both ways", panel.UserInfo{SpeedLimit: 10}, 1250000, 1250000},
		{"bps overrides mbps", panel.UserInfo{SpeedLimit: 10, SpeedLimitBps: 500000}, 500000, 500000},
		{"separate upload limit", panel.UserInfo{SpeedLimitBps: 500000, SpeedLimitUpBps: i64(100000)}, 500000, 100000},
		{"upload explicitly unlimited", panel.UserInfo{SpeedLimitBps: 500000, SpeedLimitUpBps: i64(0)}, 500000, 0},
		{"no limit", panel.UserInfo{}, 0, 0},
	}
	for _, c := range cases {
		if d, u := c.u.SpeedLimitBytes(), c.u.SpeedLimitUpBytes(); d != c.down || u != c.up {
			t.Errorf("%s: got down %d up %d, want %d %d", c.name, d, u, c.down, c.up)
		}
	}
}

func newTestLimiter(users []panel.UserInfo, client *panel.Client) *Limiter {
	Init()
	return AddLimiter("vless", tag, users, map[int]int{}, client)
}

func TestCheckLimitUnknownUserRejected(t *testing.T) {
	l := newTestLimiter([]panel.UserInfo{{Id: 1, Uuid: "a"}}, nil)
	if _, reject := l.CheckLimit(context.Background(), format.UserTag(tag, "nope"), "1.1.1.1"); !reject {
		t.Fatal("unknown user was not rejected")
	}
}

func TestCheckLimitBuckets(t *testing.T) {
	l := newTestLimiter([]panel.UserInfo{
		{Id: 1, Uuid: "free"},
		{Id: 2, Uuid: "lim", SpeedLimitBps: 1000000, SpeedLimitUpBps: i64(250000)},
		{Id: 3, Uuid: "downonly", SpeedLimitBps: 1000000, SpeedLimitUpBps: i64(0)},
	}, nil)
	ctx := context.Background()

	b, reject := l.CheckLimit(ctx, format.UserTag(tag, "free"), "1.1.1.1")
	if reject || b != nil {
		t.Fatalf("unlimited user: bucket %v reject %v, want nil false", b, reject)
	}

	b1, reject := l.CheckLimit(ctx, format.UserTag(tag, "lim"), "1.1.1.1")
	if reject || b1 == nil {
		t.Fatalf("limited user: bucket %v reject %v", b1, reject)
	}
	if got := b1.Down.Get().Rate(); got != 1000000 {
		t.Errorf("down rate %v, want 1000000", got)
	}
	if got := b1.Up.Get().Rate(); got != 250000 {
		t.Errorf("up rate %v, want 250000", got)
	}
	// Every connection and device of a user shares one bucket.
	b2, _ := l.CheckLimit(ctx, format.UserTag(tag, "lim"), "2.2.2.2")
	if b1 != b2 {
		t.Error("second connection of the same user got a different bucket")
	}

	b3, _ := l.CheckLimit(ctx, format.UserTag(tag, "downonly"), "1.1.1.1")
	if b3 == nil || b3.Up.Get() != nil || b3.Down.Get() == nil {
		t.Errorf("download-only limit: up %v down %v", b3.Up.Get(), b3.Down.Get())
	}
}

func TestUpdateUserAppliesLiveToExistingBucket(t *testing.T) {
	u := panel.UserInfo{Id: 2, Uuid: "lim", SpeedLimitBps: 1000000}
	l := newTestLimiter([]panel.UserInfo{u}, nil)
	key := format.UserTag(tag, "lim")
	b, _ := l.CheckLimit(context.Background(), key, "1.1.1.1")

	u.SpeedLimitBps = 3000000
	l.UpdateUser(tag, nil, nil, []panel.UserInfo{u})
	if got := b.Down.Get().Rate(); got != 3000000 {
		t.Errorf("after update, held bucket rate %v, want 3000000", got)
	}
	b2, _ := l.CheckLimit(context.Background(), key, "1.1.1.1")
	if b2 != b {
		t.Error("update replaced the user's bucket instead of updating it")
	}

	// limited -> unlimited drops the bucket (open links are closed by the controller)
	u.SpeedLimitBps = 0
	l.UpdateUser(tag, nil, nil, []panel.UserInfo{u})
	if b3, _ := l.CheckLimit(context.Background(), key, "1.1.1.1"); b3 != nil {
		t.Error("unlimited user still gets a bucket")
	}
}

func TestUpdateUserAddDelete(t *testing.T) {
	l := newTestLimiter([]panel.UserInfo{{Id: 1, Uuid: "a"}}, nil)
	l.UpdateUser(tag, []panel.UserInfo{{Id: 2, Uuid: "b", SpeedLimit: 5}}, []panel.UserInfo{{Id: 1, Uuid: "a"}}, nil)
	if _, reject := l.CheckLimit(context.Background(), format.UserTag(tag, "a"), "1.1.1.1"); !reject {
		t.Error("deleted user still admitted")
	}
	b, reject := l.CheckLimit(context.Background(), format.UserTag(tag, "b"), "1.1.1.1")
	if reject || b == nil || b.Down.Get().Rate() != 625000 {
		t.Errorf("added user: bucket %v reject %v", b, reject)
	}
}

func TestDynamicSpeedLimitAndExpiry(t *testing.T) {
	l := newTestLimiter([]panel.UserInfo{{Id: 1, Uuid: "a", SpeedLimit: 100}}, nil)
	key := format.UserTag(tag, "a")
	if err := l.UpdateDynamicSpeedLimit(tag, "a", 10, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	b, _ := l.CheckLimit(context.Background(), key, "1.1.1.1")
	if got := b.Down.Get().Rate(); got != 1250000 {
		t.Errorf("dynamic limit: rate %v, want 1250000 (10 Mbps, lower than the user's 100)", got)
	}

	// An expired dynamic limit must fall back to the user's own limit and
	// must not make the user unknown.
	l.SpeedLimiter.Delete(key)
	_ = l.UpdateDynamicSpeedLimit(tag, "a", 10, time.Now().Add(-time.Second))
	b, reject := l.CheckLimit(context.Background(), key, "1.1.1.1")
	if reject || b == nil || b.Down.Get().Rate() != 12500000 {
		t.Fatalf("after expiry: bucket %v reject %v", b, reject)
	}
	v, _ := l.UserLimitInfo.Load(key)
	if u := v.(*UserLimitInfo); u.ExpireTime != 0 || u.DynamicSpeedLimit != 0 {
		t.Errorf("expired dynamic limit not cleared: %+v", u)
	}
	if _, reject := l.CheckLimit(context.Background(), key, "1.1.1.1"); reject {
		t.Error("user rejected on the connection after expiry")
	}
}

func TestKick(t *testing.T) {
	l := newTestLimiter([]panel.UserInfo{{Id: 7, Uuid: "a"}}, nil)
	key := format.UserTag(tag, "a")
	l.CheckLimit(context.Background(), key, "1.1.1.1")
	l.ApplyKicks(map[int]map[string]int{7: {"1.1.1.1": 60}})
	if _, reject := l.CheckLimit(context.Background(), key, "1.1.1.1"); !reject {
		t.Error("kicked ip admitted")
	}
	if _, reject := l.CheckLimit(context.Background(), key, "::ffff:1.1.1.1"); !reject {
		t.Error("kicked ip admitted via its ipv4-mapped form")
	}
	if _, reject := l.CheckLimit(context.Background(), key, "2.2.2.2"); reject {
		t.Error("other ip of the kicked user rejected")
	}
	// expired kicks are cleared on the next ApplyKicks
	l.Kicked.Store(kickKey(7, "1.1.1.1"), time.Now().Unix()-1)
	l.ApplyKicks(nil)
	if _, reject := l.CheckLimit(context.Background(), key, "1.1.1.1"); reject {
		t.Error("ip still rejected after its kick expired")
	}
}

// claimPanel is a panel whose claimDevice keeps a per-uid ip set, like the
// real Redis-backed one.
type claimPanel struct {
	mu     sync.Mutex
	ips    map[int]map[string]bool
	calls  atomic.Int64
	status int
	delay  time.Duration
}

func (p *claimPanel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.calls.Add(1)
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	if p.status != 0 {
		w.WriteHeader(p.status)
		return
	}
	var req struct {
		UID         int    `json:"uid"`
		IP          string `json:"ip"`
		DeviceLimit int    `json:"device_limit"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	p.mu.Lock()
	defer p.mu.Unlock()
	set := p.ips[req.UID]
	if set == nil {
		set = map[string]bool{}
		p.ips[req.UID] = set
	}
	allow := set[req.IP] || len(set) < req.DeviceLimit
	if allow {
		set[req.IP] = true
	}
	json.NewEncoder(w).Encode(map[string]bool{"allow": allow})
}

func newClaimLimiter(t *testing.T, p *claimPanel) *Limiter {
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	retry := 0
	c, err := panel.New(&conf.NodeConfig{APIHost: srv.URL, NodeID: 1, Key: "k", NodeType: "vless", RetryCount: &retry, Timeout: 10})
	if err != nil {
		t.Fatal(err)
	}
	return newTestLimiter([]panel.UserInfo{{Id: 1, Uuid: "a", DeviceLimit: 2}}, c)
}

func TestDeviceLimit(t *testing.T) {
	p := &claimPanel{ips: map[int]map[string]bool{}}
	l := newClaimLimiter(t, p)
	key := format.UserTag(tag, "a")
	ctx := context.Background()
	for _, ip := range []string{"1.1.1.1", "2.2.2.2"} {
		if _, reject := l.CheckLimit(ctx, key, ip); reject {
			t.Fatalf("device %s rejected under the limit", ip)
		}
	}
	if _, reject := l.CheckLimit(ctx, key, "3.3.3.3"); !reject {
		t.Fatal("third device admitted with device_limit 2")
	}
	// An admitted ip is only claimed once per report cycle.
	before := p.calls.Load()
	for i := 0; i < 10; i++ {
		l.CheckLimit(ctx, key, "1.1.1.1")
	}
	if n := p.calls.Load() - before; n != 0 {
		t.Errorf("admitted ip re-claimed %d times in the same cycle", n)
	}
	// The report cycle resets the per-cycle set, forcing a re-claim.
	online, _ := l.GetOnlineDevice()
	if len(*online) != 2 {
		t.Errorf("online devices reported %d, want 2", len(*online))
	}
	l.CheckLimit(ctx, key, "1.1.1.1")
	if n := p.calls.Load() - before; n != 1 {
		t.Errorf("claims after cycle reset = %d, want 1", n)
	}
}

func TestDeviceLimitFailsClosed(t *testing.T) {
	key := format.UserTag(tag, "a")
	p := &claimPanel{ips: map[int]map[string]bool{}, status: 500}
	l := newClaimLimiter(t, p)
	if _, reject := l.CheckLimit(context.Background(), key, "1.1.1.1"); !reject {
		t.Error("device admitted while the panel's claim endpoint fails")
	}
	if online, _ := l.GetOnlineDevice(); len(*online) != 0 {
		t.Errorf("rejected device reported online: %v", *online)
	}

	p2 := &claimPanel{ips: map[int]map[string]bool{}, delay: claimDeviceTimeout + time.Second}
	l2 := newClaimLimiter(t, p2)
	start := time.Now()
	_, reject := l2.CheckLimit(context.Background(), key, "1.1.1.1")
	if el := time.Since(start); !reject || el > claimDeviceTimeout+time.Second {
		t.Errorf("slow panel: reject %v after %v, want reject within ~%v", reject, el, claimDeviceTimeout)
	}
}

func TestDeviceLimitChangeForcesReclaim(t *testing.T) {
	p := &claimPanel{ips: map[int]map[string]bool{}}
	l := newClaimLimiter(t, p)
	key := format.UserTag(tag, "a")
	l.CheckLimit(context.Background(), key, "1.1.1.1")
	before := p.calls.Load()
	l.UpdateUser(tag, nil, nil, []panel.UserInfo{{Id: 1, Uuid: "a", DeviceLimit: 1}})
	l.CheckLimit(context.Background(), key, "1.1.1.1")
	if p.calls.Load()-before != 1 {
		t.Error("device limit change did not force the next connection through claimDevice")
	}
}
