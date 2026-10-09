package panel

import (
	"context"
	"errors"
	"strconv"
)

// ReportUninstall tells the panel this node was uninstalled, so it shows as
// not installed instead of faulty.
func (c *Client) ReportUninstall(ctx context.Context) error {
	const path = "/api/v1/server/UniProxy/uninstall"
	r, err := c.client.R().
		SetContext(ctx).
		ForceContentType("application/json").
		Post(path)
	if err != nil {
		return err
	}
	if r == nil || r.RawResponse == nil || r.StatusCode() >= 399 {
		status := -1
		if r != nil {
			status = r.StatusCode()
		}
		return errors.New("report uninstall: unexpected status " + strconv.Itoa(status))
	}
	return nil
}
