package panel

import (
	"context"
	"errors"
	"strconv"
)

// BehaviorRecord is one completed connection's behavior record, as reported
// to the panel for user-behavior auditing.
type BehaviorRecord struct {
	UID         int    `json:"uid"`
	SourceIP    string `json:"source_ip"`
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
	r, err := c.client.R().
		SetContext(ctx).
		SetBody(records).
		ForceContentType("application/json").
		Post(path)
	if err != nil {
		return err
	}
	// resty returns no err for HTTP error statuses, and the caller has already drained
	// the buffer, so a 400/500 from the panel would silently lose the records.
	if r == nil || r.RawResponse == nil || r.StatusCode() >= 399 {
		status := -1
		if r != nil {
			status = r.StatusCode()
		}
		return errors.New("report behavior log: unexpected status " + strconv.Itoa(status))
	}
	return nil
}
