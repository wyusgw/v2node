package dispatcher

import (
	"sync/atomic"
	"time"

	"github.com/wyusgw/v2node/common/behavior"
	"github.com/xtls/xray-core/common/net"
)

type behaviorTracker struct {
	tag         string
	email       string
	sourceIP    string
	connectedAt time.Time
	up          atomic.Int64
	down        atomic.Int64
}

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
