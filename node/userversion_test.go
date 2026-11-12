package node

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	panel "github.com/wyusgw/v2node/api/v2board"
	"github.com/wyusgw/v2node/common/task"
	"github.com/wyusgw/v2node/conf"
)

func TestUserVersionTriggersPull(t *testing.T) {
	var version atomic.Value
	version.Store("")
	var status atomic.Int64
	status.Store(http.StatusOK)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/server/UniProxy/userVersion" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(int(status.Load()))
		w.Write([]byte(`{"version":"` + version.Load().(string) + `"}`))
	}))
	defer srv.Close()
	retry := 0
	api, err := panel.New(&conf.NodeConfig{APIHost: srv.URL, NodeID: 1, Key: "k", RetryCount: &retry})
	if err != nil {
		t.Fatal(err)
	}
	var pulls atomic.Int64
	c := &Controller{apiClient: api, tag: "t"}
	c.nodeInfoMonitorPeriodic = &task.Task{
		Name:     "pull",
		Interval: time.Hour,
		Execute: func(context.Context) error {
			pulls.Add(1)
			return nil
		},
	}
	c.nodeInfoMonitorPeriodic.Start(false)
	defer c.nodeInfoMonitorPeriodic.Close()
	waitPulls := func(want int64) {
		t.Helper()
		for t0 := time.Now(); time.Since(t0) < time.Second; time.Sleep(5 * time.Millisecond) {
			if pulls.Load() == want {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		if got := pulls.Load(); got != want {
			t.Fatalf("pulls = %d, want %d", got, want)
		}
	}
	ctx := context.Background()

	c.userVersionMonitor(ctx)
	c.userVersionMonitor(ctx)
	waitPulls(0)

	version.Store("a")
	start := time.Now()
	c.userVersionMonitor(ctx)
	waitPulls(1)
	t.Logf("pull started %v after the version changed", time.Since(start))
	c.userVersionMonitor(ctx)
	waitPulls(1)

	status.Store(http.StatusInternalServerError)
	c.userVersionMonitor(ctx)
	if c.userVersionSkip != userVersionBackoff {
		t.Fatalf("skip = %d after a failed request", c.userVersionSkip)
	}
	status.Store(http.StatusOK)
	version.Store("b")
	for i := 0; i < userVersionBackoff; i++ {
		c.userVersionMonitor(ctx)
	}
	waitPulls(1)
	c.userVersionMonitor(ctx)
	waitPulls(2)
}
