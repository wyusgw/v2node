package panel

import (
	"context"
	"errors"
	"strconv"
)

func (c *Client) ReportFault(ctx context.Context, message string) error {
	const path = "/api/v1/server/UniProxy/fault"
	r, err := c.client.R().
		SetContext(ctx).
		SetBody(map[string]string{"message": message}).
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
		return errors.New("report fault: unexpected status " + strconv.Itoa(status))
	}
	return nil
}
