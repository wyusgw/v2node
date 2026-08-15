package panel

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/go-resty/resty/v2"
	"github.com/wyusgw/v2node/conf"
)

// Panel is the interface for different panel's api.

type Client struct {
	client           *resty.Client
	APIHost          string
	Token            string
	NodeId           int
	NodeType         string
	nodeEtag         string
	userEtag         string
	responseBodyHash string
	UserList         *UserListBody
	AliveMap         *AliveMap
}

// nodeTypes are the values this panel accepts in node_type. It looks the id up
// in the matching table, and normalizes "hysteria2" to "hysteria" and "v2ray"
// to "vmess" on its own, so v2node's own protocol names go through unchanged.
var nodeTypes = map[string]bool{
	"vless":       true,
	"vmess":       true,
	"trojan":      true,
	"shadowsocks": true,
	"hysteria2":   true,
	"tuic":        true,
	"anytls":      true,
	"mieru":       true,
	"wireguard":   true,
}

// normalizeNodeType rejects a missing or unknown NodeType up front. The panel
// answers an unknown node_type with a 500 "server is not exist" that says
// nothing about the cause, so it is worth failing loudly here instead.
func normalizeNodeType(nodeType string) (string, error) {
	nodeType = strings.ToLower(strings.TrimSpace(nodeType))
	if nodeType == "" {
		return "", errors.New("NodeType is required, it tells the panel which protocol table to look NodeID up in")
	}
	if !nodeTypes[nodeType] {
		return "", fmt.Errorf("unsupported NodeType: %s", nodeType)
	}
	return nodeType, nil
}

func New(c *conf.NodeConfig) (*Client, error) {
	nodeType, err := normalizeNodeType(c.NodeType)
	if err != nil {
		return nil, err
	}
	client := resty.New()
	retryCount := conf.DefaultNodeRetryCount
	if c.RetryCount != nil {
		retryCount = *c.RetryCount
	}
	client.SetRetryCount(retryCount)
	client.SetHeader("User-Agent", fmt.Sprintf("v2node go-resty/%s (https://github.com/go-resty/resty)", resty.Version))
	if c.Timeout > 0 {
		client.SetTimeout(time.Duration(c.Timeout) * time.Second)
	} else {
		client.SetTimeout(time.Duration(conf.DefaultNodeTimeout) * time.Second)
	}
	client.OnError(func(req *resty.Request, err error) {
		var v *resty.ResponseError
		if errors.As(err, &v) {
			// v.Response contains the last response from the server
			// v.Err contains the original error
			logrus.Error(v.Err)
		}
	})
	client.SetBaseURL(c.APIHost)
	// set params
	client.SetQueryParams(map[string]string{
		"node_type": nodeType,
		"node_id":   strconv.Itoa(c.NodeID),
		"token":     c.Key,
	})
	return &Client{
		client:   client,
		Token:    c.Key,
		APIHost:  c.APIHost,
		NodeId:   c.NodeID,
		NodeType: nodeType,
		UserList: &UserListBody{},
		AliveMap: &AliveMap{},
	}, nil
}
