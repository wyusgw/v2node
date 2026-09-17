package dispatcher

import (
	"sync/atomic"
	"time"

	"github.com/wyusgw/v2node/common/behavior"
	"github.com/xtls/xray-core/common/net"
)

// behaviorTracker accumulates one connection's per-connection upload/download
// byte counts (via the same XrayTrafficCounter/SizeStatWriter wiring already
// used for the per-user aggregate counters, just pointed at connection-local
// atomics instead of the shared per-user ones) so the totals can be read back
// once the connection finishes.
type behaviorTracker struct {
	tag         string
	email       string
	sourceIP    string
	connectedAt time.Time
	up          atomic.Int64
	down        atomic.Int64
}

// finish records the completed connection against destination (the final,
// post-sniffing destination) once the proxy handler has returned - i.e. once
// the connection has actually closed, so Duration/Upload/Download reflect the
// whole connection rather than a snapshot mid-flight.
func (t *behaviorTracker) finish(destination net.Destination) {
	if t == nil {
		return
	}
	behavior.Record(t.tag, behavior.Entry{
		Email:       t.email,
		SourceIP:    t.sourceIP,
		Domain:      destination.Address.String(),
		Port:        int(destination.Port.Value()),
		Network:     destination.Network.SystemString(),
		ConnectedAt: t.connectedAt,
		Duration:    time.Since(t.connectedAt),
		Upload:      t.up.Load(),
		Download:    t.down.Load(),
	})
}
