package panel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"encoding/json/jsontext"
	"encoding/json/v2"

	"github.com/vmihailenco/msgpack/v5"
)

type OnlineUser struct {
	UID int
	IP  string
}

type UserInfo struct {
	Id            int    `json:"id" msgpack:"id"`
	Uuid          string `json:"uuid" msgpack:"uuid"`
	SpeedLimit    int    `json:"speed_limit" msgpack:"speed_limit"`
	SpeedLimitBps int64  `json:"speed_limit_bps" msgpack:"speed_limit_bps"`
	// nil: the panel predates separate limits, and the one limit applies both ways.
	SpeedLimitUpBps *int64 `json:"speed_limit_up_bps" msgpack:"speed_limit_up_bps"`
	DeviceLimit     int    `json:"device_limit" msgpack:"device_limit"`
	SpeedLimitGroup int    `json:"speed_limit_group" msgpack:"speed_limit_group"`
}

func (u UserInfo) SpeedLimitBytes() int64 {
	if u.SpeedLimitBps > 0 {
		return u.SpeedLimitBps
	}
	return MbpsToBytes(u.SpeedLimit)
}

func (u UserInfo) SpeedLimitUpBytes() int64 {
	if u.SpeedLimitUpBps != nil {
		return *u.SpeedLimitUpBps
	}
	return u.SpeedLimitBytes()
}

func MbpsToBytes(mbps int) int64 {
	return int64(mbps) * 1000000 / 8
}

type UserListBody struct {
	Users []UserInfo `json:"users" msgpack:"users"`
}

type AliveMap struct {
	Alive map[int]int            `json:"alive"` // Key: Uid, value: alive_ip
	Kick  map[int]map[string]int `json:"kick"`  // Key: Uid, value: {Key: Ip, value: seconds blocked}
}

// GetUserList will pull user from v2board
func (c *Client) GetUserList(ctx context.Context) ([]UserInfo, error) {
	const path = "/api/v1/server/UniProxy/user"
	r, err := c.client.R().
		SetContext(ctx).
		SetHeader("If-None-Match", c.userEtag).
		SetHeader("X-Response-Format", "msgpack").
		SetDoNotParseResponse(true).
		Get(path)
	if err != nil {
		return nil, err
	}
	if r == nil || r.RawResponse == nil {
		return nil, fmt.Errorf("received nil response or raw response")
	}
	defer r.RawResponse.Body.Close()

	if r.StatusCode() == 304 {
		return nil, nil
	}
	userlist := &UserListBody{}
	if strings.Contains(r.Header().Get("Content-Type"), "application/x-msgpack") {
		decoder := msgpack.NewDecoder(r.RawResponse.Body)
		if err := decoder.Decode(userlist); err != nil {
			return nil, fmt.Errorf("decode user list error: %w", err)
		}
	} else {
		dec := jsontext.NewDecoder(r.RawResponse.Body)
		for {
			tok, err := dec.ReadToken()
			if err != nil {
				return nil, fmt.Errorf("decode user list error: %w", err)
			}
			if tok.Kind() == '"' && tok.String() == "users" {
				break
			}
		}
		tok, err := dec.ReadToken()
		if err != nil {
			return nil, fmt.Errorf("decode user list error: %w", err)
		}
		if tok.Kind() != '[' {
			return nil, fmt.Errorf(`decode user list error: expected "users" array`)
		}
		for dec.PeekKind() != ']' {
			val, err := dec.ReadValue()
			if err != nil {
				return nil, fmt.Errorf("decode user list error: read user object: %w", err)
			}
			var u UserInfo
			if err := json.Unmarshal(val, &u); err != nil {
				return nil, fmt.Errorf("decode user list error: unmarshal user error: %w", err)
			}
			userlist.Users = append(userlist.Users, u)
		}
	}
	c.userEtag = r.Header().Get("ETag")
	return userlist.Users, nil
}

// GetUserAlive will fetch the alive_ip count for users
func (c *Client) GetUserAlive(ctx context.Context) (map[int]int, error) {
	c.AliveMap = &AliveMap{}
	const path = "/api/v1/server/UniProxy/alivelist"
	r, err := c.client.R().
		SetContext(ctx).
		ForceContentType("application/json").
		Get(path)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		c.AliveMap.Alive = make(map[int]int)
		return c.AliveMap.Alive, nil
	}
	if r == nil || r.RawResponse == nil || r.StatusCode() >= 399 {
		c.AliveMap.Alive = make(map[int]int)
		return c.AliveMap.Alive, nil
	}
	defer r.RawResponse.Body.Close()
	if err := json.Unmarshal(r.Body(), c.AliveMap); err != nil {
		fmt.Printf("unmarshal user alive list error: %s", err)
		c.AliveMap.Alive = make(map[int]int)
	}

	return c.AliveMap.Alive, nil
}

type UserTraffic struct {
	UID      int
	Upload   int64
	Download int64
}

// ReportUserTraffic reports the user traffic
func (c *Client) ReportUserTraffic(ctx context.Context, userTraffic []UserTraffic) error {
	data := make(map[int][]int64, len(userTraffic))
	for i := range userTraffic {
		data[userTraffic[i].UID] = []int64{userTraffic[i].Upload, userTraffic[i].Download}
	}
	const path = "/api/v1/server/UniProxy/push"
	_, err := c.client.R().
		SetContext(ctx).
		SetBody(data).
		ForceContentType("application/json").
		Post(path)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) ReportNodeOnlineUsers(ctx context.Context, data *map[int][]string) error {
	const path = "/api/v1/server/UniProxy/alive"
	_, err := c.client.R().
		SetContext(ctx).
		SetBody(data).
		ForceContentType("application/json").
		Post(path)

	if err != nil {
		return err
	}

	return nil
}

type claimDeviceResult struct {
	Allow bool `json:"allow"`
}

// Callers must treat an error as a reject (fail-closed).
func (c *Client) ClaimDevice(ctx context.Context, uid int, ip string, deviceLimit int) (bool, error) {
	const path = "/api/v1/server/UniProxy/claimDevice"
	r, err := c.client.R().
		SetContext(ctx).
		SetBody(map[string]interface{}{
			"uid":          uid,
			"ip":           ip,
			"device_limit": deviceLimit,
		}).
		ForceContentType("application/json").
		Post(path)
	if err != nil {
		return false, err
	}
	if r == nil || r.RawResponse == nil || r.StatusCode() >= 399 {
		return false, fmt.Errorf("claim device: unexpected response")
	}
	var result claimDeviceResult
	if err := json.Unmarshal(r.Body(), &result); err != nil {
		return false, fmt.Errorf("unmarshal claim device response: %w", err)
	}
	return result.Allow, nil
}

type userVersionBody struct {
	Version string `json:"version"`
}

func (c *Client) GetUserVersion(ctx context.Context) (string, error) {
	const path = "/api/v1/server/UniProxy/userVersion"
	r, err := c.client.R().
		SetContext(ctx).
		ForceContentType("application/json").
		Get(path)
	if err != nil {
		return "", err
	}
	if r == nil || r.RawResponse == nil || r.StatusCode() >= 399 {
		return "", fmt.Errorf("get user version: unexpected response")
	}
	var body userVersionBody
	if err := json.Unmarshal(r.Body(), &body); err != nil {
		return "", fmt.Errorf("unmarshal user version: %w", err)
	}
	return body.Version, nil
}
