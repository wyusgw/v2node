package conf

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

const DefaultNodeRetryCount = 1
const DefaultNodeTimeout = 15

type Conf struct {
	LogConfig        LogConfig        `mapstructure:"Log"`
	ConnectionConfig ConnectionConfig `mapstructure:"Connection"`
	NodeConfigs      []NodeConfig     `mapstructure:"Nodes"`
	PprofPort        int              `mapstructure:"PprofPort"`
}

// ConnectionConfig is the per-connection policy applied to every inbound.
// Fields mean the same as xray's policy handshake, connIdle, uplinkOnly,
// downlinkOnly (seconds) and bufferSize (KB).
type ConnectionConfig struct {
	Handshake    uint32 `mapstructure:"Handshake"`
	ConnIdle     uint32 `mapstructure:"ConnIdle"`
	UplinkOnly   uint32 `mapstructure:"UplinkOnly"`
	DownlinkOnly uint32 `mapstructure:"DownlinkOnly"`
	// BufferSize is how much data, in KB, each connection may buffer in
	// the node on top of the kernel socket buffers. Every connection whose
	// client reads slower than the target sends fills it completely, and
	// the kernel buffers already absorb bursts, so larger values cost
	// memory without adding throughput. Only protocols that relay through
	// an internal pipe (vmess, trojan, shadowsocks) use it.
	BufferSize int32 `mapstructure:"BufferSize"`
}

type LogConfig struct {
	Level  string `mapstructure:"Level"`
	Output string `mapstructure:"Output"`
	Access string `mapstructure:"Access"`
}

type NodeConfig struct {
	APIHost string `mapstructure:"ApiHost"`
	NodeID  int    `mapstructure:"NodeID"`
	// NodeType is optional. Leave it empty (or set it to "v2node") to let the
	// panel's unified v2node table tell the node which protocol to run via
	// the config API's "protocol" field. Set it to a specific protocol name
	// (vmess/vless/trojan/shadowsocks/hysteria2/tuic/anytls/mieru) instead to
	// pin this node to that protocol's own table, bypassing the v2node table.
	NodeType   string `mapstructure:"NodeType"`
	Key        string `mapstructure:"ApiKey"`
	Timeout    int    `mapstructure:"Timeout"`
	RetryCount *int   `mapstructure:"RetryCount"`
}

func New() *Conf {
	return &Conf{
		LogConfig: LogConfig{
			Level:  "info",
			Output: "",
			Access: "none",
		},
		ConnectionConfig: ConnectionConfig{
			Handshake:    4,
			ConnIdle:     120,
			UplinkOnly:   2,
			DownlinkOnly: 4,
			BufferSize:   8,
		},
	}
}

func (p *Conf) LoadFromPath(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open config file error: %s", err)
	}
	defer f.Close()
	v := viper.New()
	v.SetConfigFile(filePath)
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config file error: %s", err)
	}
	if err := v.Unmarshal(p); err != nil {
		return fmt.Errorf("unmarshal config error: %s", err)
	}
	for i := range p.NodeConfigs {
		if p.NodeConfigs[i].RetryCount == nil {
			p.NodeConfigs[i].RetryCount = intPtr(DefaultNodeRetryCount)
		}
	}
	return nil
}

func intPtr(v int) *int {
	return &v
}
