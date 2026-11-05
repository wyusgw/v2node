package node

import (
	"context"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const faultReportTimeout = 10 * time.Second

// setFault records a fault under its source and syncs it to the panel.
func (c *Controller) setFault(source, message string) {
	c.faultMu.Lock()
	if c.faults == nil {
		c.faults = make(map[string]string)
	}
	c.faults[source] = message
	c.faultMu.Unlock()
	c.syncFault()
}

// clearFault drops the fault of one source and syncs the remaining ones.
func (c *Controller) clearFault(source string) {
	c.faultMu.Lock()
	delete(c.faults, source)
	c.faultMu.Unlock()
	c.syncFault()
}

// syncFault sends the current fault set unless the panel already has it. The first sync always
// sends, clearing a fault left by a crashed run; a failed send is retried by the next sync.
// Best effort: an unreachable or older panel only logs.
func (c *Controller) syncFault() {
	c.faultMu.Lock()
	defer c.faultMu.Unlock()
	parts := make([]string, 0, len(c.faults))
	for source, message := range c.faults {
		parts = append(parts, source+": "+message)
	}
	sort.Strings(parts)
	desired := strings.Join(parts, "; ")
	if c.faultSynced && desired == c.faultReported {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), faultReportTimeout)
	defer cancel()
	if err := c.apiClient.ReportFault(ctx, desired); err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Debug("Report fault failed")
		return
	}
	c.faultSynced = true
	c.faultReported = desired
}

// SetFault records a fault on every node, for failures that happen before or
// outside any single controller (e.g. the core failing to start).
func (n *Node) SetFault(source, message string) {
	for _, c := range n.controllers {
		c.setFault(source, message)
	}
}
