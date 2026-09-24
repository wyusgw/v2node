package dispatcher

import (
	"context"
	"testing"
	"time"

	panel "github.com/wyusgw/v2node/api/v2board"
	"github.com/wyusgw/v2node/common/counter"
	"github.com/wyusgw/v2node/common/format"
	"github.com/wyusgw/v2node/limiter"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
)

func pump(t *testing.T, w buf.Writer, r buf.Reader, total int) time.Duration {
	t.Helper()
	errc := make(chan error, 1)
	start := time.Now()
	go func() {
		for sent := 0; sent < total; {
			b := buf.New()
			n := min(int(buf.Size), total-sent)
			b.Extend(int32(n))
			if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				errc <- err
				return
			}
			sent += n
		}
		errc <- nil
	}()
	for got := 0; got < total; {
		mb, err := r.ReadMultiBuffer()
		if err != nil {
			t.Errorf("read: %v", err)
			return time.Since(start)
		}
		got += int(mb.Len())
		buf.ReleaseMulti(mb)
	}
	elapsed := time.Since(start)
	if err := <-errc; err != nil {
		t.Errorf("write: %v", err)
	}
	return elapsed
}

func checkRate(t *testing.T, name string, total int, rate int64, elapsed time.Duration) {
	t.Helper()
	want := time.Duration(float64(total) / float64(rate) * float64(time.Second))
	t.Logf("%s: %d bytes in %v = %.0f B/s (limit %d B/s)", name, total,
		elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds(), rate)
	if elapsed < want*85/100 || elapsed > want*120/100 {
		t.Errorf("%s: took %v, want about %v", name, elapsed, want)
	}
}

// End to end through limiter.CheckLimit and getLink: a user with different
// upload and download limits must get each on the matching direction, both
// at the same time, and a limit change from the panel must reach the
// already-open connection.
func TestGetLinkUpDownSpeedLimit(t *testing.T) {
	const tag = "speedtest"
	const up, down int64 = 1 << 20, 4 << 20 // 1 MiB/s up, 4 MiB/s down
	upBps := up
	user := panel.UserInfo{Id: 1, Uuid: "uuid-1", SpeedLimitBps: down, SpeedLimitUpBps: &upBps}
	limiter.Init()
	l := limiter.AddLimiter("vless", tag, []panel.UserInfo{user}, map[int]int{}, nil)
	defer limiter.DeleteLimiter(tag)

	email := format.UserTag(tag, user.Uuid)
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		Tag:    tag,
		User:   &protocol.MemoryUser{Email: email},
		Source: net.TCPDestination(net.ParseAddress("192.0.2.1"), 40000),
	})
	d := &DefaultDispatcher{}
	in, out, _, _, err := d.getLink(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Upload: client -> inbound.Writer -> outbound.Reader -> target.
	// Download: target -> outbound.Writer -> inbound.Reader -> client.
	upc, downc := make(chan time.Duration, 1), make(chan time.Duration, 1)
	go func() { upc <- pump(t, in.Writer, out.Reader, int(up)) }()
	go func() { downc <- pump(t, out.Writer, in.Reader, int(down)) }()
	checkRate(t, "upload", int(up), up, <-upc)
	checkRate(t, "download", int(down), down, <-downc)

	v, _ := d.Counter.Load(tag)
	tc := v.(*counter.TrafficCounter)
	if got := tc.GetUpCount(email); got != up {
		t.Errorf("upload counter = %d, want %d", got, up)
	}
	if got := tc.GetDownCount(email); got != down {
		t.Errorf("download counter = %d, want %d", got, down)
	}

	// Panel swaps the limits (4 MiB/s up, 1 MiB/s down); the open link
	// must follow without reconnecting.
	newUp := down
	changed := user
	changed.SpeedLimitBps, changed.SpeedLimitUpBps = up, &newUp
	l.UpdateUser(tag, nil, nil, []panel.UserInfo{changed})
	go func() { upc <- pump(t, in.Writer, out.Reader, int(down)) }()
	go func() { downc <- pump(t, out.Writer, in.Reader, int(up)) }()
	checkRate(t, "upload after update", int(down), down, <-upc)
	checkRate(t, "download after update", int(up), up, <-downc)
}
