package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	panel "github.com/wyusgw/v2node/api/v2board"
)

func uuids(removed []ssRemovedUser) map[string]string {
	m := make(map[string]string, len(removed))
	for _, r := range removed {
		m[r.Uuid] = r.Cipher
	}
	return m
}

func TestRestoredSSRemoved(t *testing.T) {
	now := time.Now()
	state := &ssUserState{
		Cipher: "aes-256-gcm",
		Users:  []string{"a", "b", "c"},
		Removed: []ssRemovedUser{
			{Uuid: "old", Cipher: "aes-128-gcm", At: now.Add(-ssRemovedMaxAge - time.Hour).Unix()},
			{Uuid: "x", Cipher: "aes-128-gcm", At: now.Add(-time.Hour).Unix()},
			{Uuid: "back", Cipher: "aes-256-gcm", At: now.Add(-time.Hour).Unix()},
		},
	}
	current := []panel.UserInfo{{Uuid: "a"}, {Uuid: "back"}}
	got := uuids(restoredSSRemoved(state, current, now))
	want := map[string]string{
		"x": "aes-128-gcm", // removed while running, kept with its cipher
		"b": "aes-256-gcm", // removed while down
		"c": "aes-256-gcm",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestUpdatedSSRemoved(t *testing.T) {
	now := time.Now()
	removed := []ssRemovedUser{{Uuid: "a", Cipher: "c1", At: now.Unix()}}
	removed = updatedSSRemoved(removed,
		[]panel.UserInfo{{Uuid: "b"}, {Uuid: "c"}},
		[]panel.UserInfo{{Uuid: "a"}, {Uuid: "c"}}, // a added back; c both (modified)
		"c2", now)
	got := uuids(removed)
	if len(got) != 1 || got["b"] != "c2" {
		t.Fatalf("got %v", got)
	}
}

func TestPruneSSRemovedLimit(t *testing.T) {
	now := time.Now()
	removed := make([]ssRemovedUser, ssRemovedMax+5)
	for i := range removed {
		removed[i] = ssRemovedUser{Uuid: string(rune('a' + i%26)), At: now.Unix()}
	}
	removed[len(removed)-1].Uuid = "newest"
	removed = pruneSSRemoved(removed, now)
	if len(removed) != ssRemovedMax || removed[len(removed)-1].Uuid != "newest" {
		t.Fatalf("kept %d, newest %q", len(removed), removed[len(removed)-1].Uuid)
	}
}

func TestSaveSSStatePrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ss-users.json")
	if err := os.WriteFile(path, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}
	state := &ssUserState{Cipher: "aes-128-gcm", Users: []string{"u1"}, Removed: []ssRemovedUser{{Uuid: "u2", Cipher: "aes-128-gcm", At: 1}}}
	if err := saveSSState(path, state); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode %v, want 0600", info.Mode().Perm())
	}
	got, err := loadSSState(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cipher != state.Cipher || len(got.Users) != 1 || len(got.Removed) != 1 || got.Removed[0].Uuid != "u2" {
		t.Fatalf("loaded %+v", got)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temporary file left behind")
	}
}

func TestSSStatePathPerNode(t *testing.T) {
	a := &Controller{conf: nil}
	if filepath.Dir(a.ssStatePath()) != "/etc/v2node" {
		t.Fatalf("default dir: %s", a.ssStatePath())
	}
}
