package core

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	panel "github.com/wyusgw/v2node/api/v2board"
	coreConf "github.com/xtls/xray-core/infra/conf"
)

func TestBuildShadowsocks2022PlaceholderKeyLength(t *testing.T) {
	for cipher, want := range map[string]int{
		"2022-blake3-aes-128-gcm": 16,
		"2022-blake3-aes-256-gcm": 32,
	} {
		info := &panel.NodeInfo{Common: &panel.CommonNode{
			Cipher:    cipher,
			ServerKey: base64.StdEncoding.EncodeToString(make([]byte, want)),
		}}
		in := &coreConf.InboundDetourConfig{}
		if err := buildShadowsocks(info, in); err != nil {
			t.Fatal(err)
		}
		settings := &coreConf.ShadowsocksServerConfig{}
		if err := json.Unmarshal(*in.Settings, settings); err != nil {
			t.Fatal(err)
		}
		key, err := base64.StdEncoding.DecodeString(settings.Users[0].Password)
		if err != nil {
			t.Fatal(err)
		}
		if len(key) != want {
			t.Errorf("%s: placeholder key is %d bytes, want %d", cipher, len(key), want)
		}
		if _, err := settings.Build(); err != nil {
			t.Errorf("%s: %v", cipher, err)
		}
	}
}
