package node

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyusgw/v2node/api/v2board"
)

// A Shadowsocks (not 2022) node has to try every user's key to find who a
// connection belongs to, so clients of removed users that keep reconnecting
// are expensive. The core recognizes them by their old keys, but only knows
// the users removed since it started. To cover users removed before a
// restart, the node saves its users and the users removed in the last week,
// and hands the removed ones to the core when it starts.
const (
	// Same as how long the core keeps keys of removed users.
	ssRemovedMaxAge = 7 * 24 * time.Hour
	ssRemovedMax    = 10000
)

type ssUserState struct {
	Cipher  string          `json:"cipher"`
	Users   []string        `json:"users"`
	Removed []ssRemovedUser `json:"removed"`
}

type ssRemovedUser struct {
	Uuid   string `json:"uuid"`
	Cipher string `json:"cipher"`
	At     int64  `json:"at"` // Unix seconds
}

func (c *Controller) isLegacySS() bool {
	return c.info != nil && c.info.Type == "shadowsocks" &&
		c.info.Common != nil && c.info.Common.ServerKey == ""
}

// ssStatePath returns the file of this node's Shadowsocks users. It holds
// user UUIDs, which are passwords, so it is only readable by its owner.
func (c *Controller) ssStatePath() string {
	dir := "/etc/v2node"
	host := ""
	id := 0
	if c.conf != nil {
		if c.conf.CertDir != "" {
			dir = c.conf.CertDir
		}
		host = c.conf.APIHost
		id = c.conf.NodeID
	}
	sum := sha256.Sum256([]byte(host))
	return filepath.Join(dir, fmt.Sprintf("ss-users-%d-%x.json", id, sum[:4]))
}

func loadSSState(path string) (*ssUserState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	state := &ssUserState{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, err
	}
	return state, nil
}

func saveSSState(path string, state *ssUserState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0600); err != nil { // an old file keeps its mode
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// pruneSSRemoved drops users removed too long ago, then the oldest ones over
// the limit. removed is oldest first.
func pruneSSRemoved(removed []ssRemovedUser, now time.Time) []ssRemovedUser {
	oldest := now.Add(-ssRemovedMaxAge).Unix()
	i := 0
	for i < len(removed) && removed[i].At < oldest {
		i++
	}
	removed = removed[i:]
	if len(removed) > ssRemovedMax {
		removed = removed[len(removed)-ssRemovedMax:]
	}
	return removed
}

// restoredSSRemoved returns the removed users to keep after a start with the
// current users: the saved removed users, and the saved users that are gone
// now, removed while the node was down. Current users are left out.
func restoredSSRemoved(state *ssUserState, current []panel.UserInfo, now time.Time) []ssRemovedUser {
	active := make(map[string]bool, len(current))
	for _, u := range current {
		active[u.Uuid] = true
	}
	seen := make(map[string]bool)
	var removed []ssRemovedUser
	for _, r := range pruneSSRemoved(state.Removed, now) {
		if !active[r.Uuid] && !seen[r.Uuid+"\x00"+r.Cipher] {
			seen[r.Uuid+"\x00"+r.Cipher] = true
			removed = append(removed, r)
		}
	}
	if state.Cipher != "" {
		for _, uuid := range state.Users {
			if !active[uuid] && !seen[uuid+"\x00"+state.Cipher] {
				seen[uuid+"\x00"+state.Cipher] = true
				removed = append(removed, ssRemovedUser{Uuid: uuid, Cipher: state.Cipher, At: now.Unix()})
			}
		}
	}
	return pruneSSRemoved(removed, now)
}

// updatedSSRemoved adds deleted users to removed and drops added ones.
func updatedSSRemoved(removed []ssRemovedUser, deleted, added []panel.UserInfo, cipher string, now time.Time) []ssRemovedUser {
	back := make(map[string]bool, len(added))
	for _, u := range added {
		back[u.Uuid] = true
	}
	kept := removed[:0:0]
	for _, r := range removed {
		if !back[r.Uuid] {
			kept = append(kept, r)
		}
	}
	for _, u := range deleted {
		if !back[u.Uuid] {
			kept = append(kept, ssRemovedUser{Uuid: u.Uuid, Cipher: cipher, At: now.Unix()})
		}
	}
	return pruneSSRemoved(kept, now)
}

func (c *Controller) saveSSUsers() {
	users := make([]string, len(c.userList))
	for i, u := range c.userList {
		users[i] = u.Uuid
	}
	state := &ssUserState{Cipher: c.info.Common.Cipher, Users: users, Removed: c.ssRemoved}
	if err := saveSSState(c.ssStatePath(), state); err != nil {
		log.WithFields(log.Fields{"tag": c.tag, "err": err}).Warn("Save shadowsocks users failed")
	}
}

// restoreSSUsers hands the users removed before this start to the core and
// saves the current users. It is called after the node's users are added.
func (c *Controller) restoreSSUsers() {
	if !c.isLegacySS() {
		return
	}
	c.ssRemoved = nil
	path := c.ssStatePath()
	state, err := loadSSState(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		log.WithFields(log.Fields{"tag": c.tag, "err": err}).Warn("Load shadowsocks users failed")
	default:
		c.ssRemoved = restoredSSRemoved(state, c.userList, time.Now())
	}
	byCipher := make(map[string][]panel.UserInfo)
	for _, r := range c.ssRemoved {
		byCipher[r.Cipher] = append(byCipher[r.Cipher], panel.UserInfo{Uuid: r.Uuid})
	}
	for cipher, users := range byCipher {
		if err := c.server.RetireSSUsers(c.tag, users, cipher); err != nil {
			log.WithFields(log.Fields{"tag": c.tag, "err": err}).Warn("Retire shadowsocks users failed")
		}
	}
	if len(c.ssRemoved) > 0 {
		log.WithField("tag", c.tag).Infof("Restored %d removed shadowsocks users", len(c.ssRemoved))
	}
	c.saveSSUsers()
}

// updateSSUsers records a user list change. c.userList must already be the
// new list.
func (c *Controller) updateSSUsers(deleted, added []panel.UserInfo) {
	if !c.isLegacySS() || len(deleted)+len(added) == 0 {
		return
	}
	c.ssRemoved = updatedSSRemoved(c.ssRemoved, deleted, added, c.info.Common.Cipher, time.Now())
	c.saveSSUsers()
}
