package node

import (
	"context"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const faultReportTimeout = 10 * time.Second

func (c *Controller) setFault(source, message string) {
	c.faultMu.Lock()
	if c.faults == nil {
		c.faults = make(map[string]string)
	}
	c.faults[source] = message
	c.faultMu.Unlock()
	c.syncFault()
}

func (c *Controller) clearFault(source string) {
	c.faultMu.Lock()
	delete(c.faults, source)
	c.faultMu.Unlock()
	c.syncFault()
}

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

func (n *Node) SetFault(source, message string) {
	for _, c := range n.controllers {
		c.setFault(source, message)
	}
}
