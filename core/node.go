package core

import (
	"fmt"

	panel "github.com/wyusgw/v2node/api/v2board"
)

func (v *V2Core) AddNode(tag string, info *panel.NodeInfo, users []panel.UserInfo) error {
	v.access.RLock()
	defer v.access.RUnlock()
	if v.ihm == nil {
		return errCoreClosed
	}
	inBoundConfig, err := buildInbound(info, tag, users)
	if err != nil {
		return fmt.Errorf("build inbound error: %s", err)
	}
	err = v.addInbound(inBoundConfig)
	if err != nil {
		return fmt.Errorf("add inbound error: %s", err)
	}
	return nil
}

func (v *V2Core) DelNode(tag string) error {
	v.access.RLock()
	defer v.access.RUnlock()
	if v.ihm == nil {
		return errCoreClosed
	}
	err := v.removeInbound(tag)
	if err != nil {
		return fmt.Errorf("remove in error: %s", err)
	}
	return nil
}
