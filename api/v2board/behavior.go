package panel

import (
	"context"
)

// BehaviorRecord is one completed connection's behavior record, as reported
// to the panel for user-behavior auditing.
type BehaviorRecord struct {
	UID         int    `json:"uid"`
	Domain      string `json:"domain"`
	Port        int    `json:"port"`
	Network     string `json:"network"`
	ConnectedAt int64  `json:"connected_at"`
	Duration    int64  `json:"duration"`
	Upload      int64  `json:"upload"`
	Download    int64  `json:"download"`
}

// ReportBehaviorLog batch-reports completed connections' behavior records
// (destination domain/port/network, connect time, duration, up/down bytes)
// to the panel.
func (c *Client) ReportBehaviorLog(ctx context.Context, records []BehaviorRecord) error {
	const path = "/api/v1/server/UniProxy/behaviorLog"
	_, err := c.client.R().
		SetContext(ctx).
		SetBody(records).
		ForceContentType("application/json").
		Post(path)
	if err != nil {
		return err
	}
	return nil
}
