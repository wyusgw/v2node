// Package behavior buffers per-connection visit records per inbound tag for the node
// controller to drain and report. Nothing is buffered for a tag until AddRecorder is called.
package behavior

import (
	"sync"
	"time"
)

// Entry is one completed connection's behavior record. Email is the raw inbound-tagged user
// identifier, as the dispatcher has no tag/uuid -> UID map; core.GetBehaviorSlice resolves it.
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
