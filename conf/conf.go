package conf

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

const DefaultNodeRetryCount = 1
const DefaultNodeTimeout = 15

type Conf struct {
	LogConfig   LogConfig    `mapstructure:"Log"`
	NodeConfigs []NodeConfig `mapstructure:"Nodes"`
	PprofPort   int          `mapstructure:"PprofPort"`
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
