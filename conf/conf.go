package conf

import (
	"fmt"
	"os"
	"path/filepath"

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
	// BufferSize is how much data, in KB, each connection may buffer on top of the kernel
	// socket buffers; larger values cost memory without adding throughput. Only protocols
	// relaying through an internal pipe (vmess, trojan, shadowsocks) use it.
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
	// NodeType is optional. Empty or "v2node" lets the panel's unified table choose the
	// protocol; a protocol name (vmess/vless/trojan/shadowsocks/hysteria/hysteria2/tuic/
	// anytls/mieru) pins this node to that protocol's own table.
	NodeType   string `mapstructure:"NodeType"`
	Key        string `mapstructure:"ApiKey"`
	Timeout    int    `mapstructure:"Timeout"`
	RetryCount *int   `mapstructure:"RetryCount"`
	// CertDir is set from the config file's own directory, not read from it.
	CertDir string `mapstructure:"-"`
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
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		absPath = filePath
	}
	certDir := filepath.Dir(absPath)
	for i := range p.NodeConfigs {
		if p.NodeConfigs[i].RetryCount == nil {
			p.NodeConfigs[i].RetryCount = intPtr(DefaultNodeRetryCount)
		}
		p.NodeConfigs[i].CertDir = certDir
	}
	return nil
}

func intPtr(v int) *int {
	return &v
}
