// Package behavior buffers per-connection "what did this user visit"
// records (domain, port, network, connect time, duration, up/down bytes)
// per inbound tag, so the node controller can periodically drain and report
// them to the panel. Recording is opt-in per tag: nothing is buffered for a
// tag unless AddRecorder has been called for it, which keeps the feature at
// zero cost when disabled (the default).
package behavior

import (
	"sync"
	"time"
)

// Entry is one completed connection's behavior record. Email is the raw
// inbound-tagged user identifier (dispatcher session user.Email, i.e.
// format.UserTag(tag, uuid)) rather than a resolved panel UID, since the
// dispatcher package has no access to the tag/uuid -> UID map maintained by
// core.V2Core; callers resolve it when draining (see core.GetBehaviorSlice).
type Entry struct {
	Email       string
	SourceIP    string
	Domain      string
	Port        int
	Network     string
	ConnectedAt time.Time
	Duration    time.Duration
	Upload      int64
	Download    int64
}

// maxBufferedEntries bounds memory per tag if the panel is unreachable for a
// while; once full, the oldest entry is dropped to make room for the newest
// rather than growing without limit.
const maxBufferedEntries = 5000

type recorder struct {
	mu      sync.Mutex
	entries []Entry
}

var registry sync.Map // map[string]*recorder, keyed by inbound tag

// AddRecorder enables behavior recording for tag. Safe to call repeatedly
// (e.g. on every controller (re)start) - it only creates the buffer once.
func AddRecorder(tag string) {
	registry.LoadOrStore(tag, &recorder{})
}

// DeleteRecorder disables behavior recording for tag and discards any
// buffered, unreported entries.
func DeleteRecorder(tag string) {
	registry.Delete(tag)
}

// IsEnabled reports whether behavior recording is currently on for tag.
func IsEnabled(tag string) bool {
	_, ok := registry.Load(tag)
	return ok
}

// Record appends e to tag's buffer. A no-op if recording isn't enabled for
// tag, so callers don't need to guard every call site with IsEnabled first.
func Record(tag string, e Entry) {
	v, ok := registry.Load(tag)
	if !ok {
		return
	}
	r := v.(*recorder)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) >= maxBufferedEntries {
		r.entries = r.entries[1:]
	}
	r.entries = append(r.entries, e)
}

// Drain removes and returns all entries currently buffered for tag.
func Drain(tag string) []Entry {
	v, ok := registry.Load(tag)
	if !ok {
		return nil
	}
	r := v.(*recorder)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) == 0 {
		return nil
	}
	out := r.entries
	r.entries = nil
	return out
}
