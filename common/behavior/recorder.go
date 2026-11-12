package behavior

import (
	"sync"
	"time"
)

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

const maxBufferedEntries = 5000

type recorder struct {
	mu      sync.Mutex
	entries []Entry
}

var registry sync.Map // map[string]*recorder, keyed by inbound tag

func AddRecorder(tag string) {
	registry.LoadOrStore(tag, &recorder{})
}

func DeleteRecorder(tag string) {
	registry.Delete(tag)
}

func IsEnabled(tag string) bool {
	_, ok := registry.Load(tag)
	return ok
}

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
