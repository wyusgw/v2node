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

// pinnedNodeTypes are the protocol-specific tables a node can be pinned to
// instead of the panel's unified v2node table. Anything outside this set
// (including empty, which defaults to "v2node") is rejected up front, since
// the panel answers an unknown node_type with a 500 "server is not exist"
// that says nothing about the cause.
var pinnedNodeTypes = map[string]bool{
	"vless":       true,
	"vmess":       true,
	"trojan":      true,
	"shadowsocks": true,
	"hysteria2":   true,
	"tuic":        true,
	"anytls":      true,
	"mieru":       true,
}

func New(c *conf.NodeConfig) (*Client, error) {
	nodeType := strings.ToLower(strings.TrimSpace(c.NodeType))
	if nodeType == "" {
		nodeType = "v2node"
	} else if nodeType != "v2node" && !pinnedNodeTypes[nodeType] {
		return nil, fmt.Errorf("unsupported NodeType: %s", nodeType)
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
