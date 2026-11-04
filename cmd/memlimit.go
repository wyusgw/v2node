package cmd

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// setMemoryLimitFromCgroup gives the Go runtime a soft memory limit of 90%
// of the cgroup's memory limit (systemd MemoryMax, docker --memory), unless
// GOMEMLIMIT already sets one. Without it the GC lets the heap grow to twice
// the live data before collecting, so a node near its limit, above all one
// whose CPU is saturated and whose GC falls behind, gets OOM-killed instead
// of collecting harder.
func setMemoryLimitFromCgroup() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	limit, ok := cgroupMemoryLimit("/proc/self/cgroup", "/sys/fs/cgroup")
	if !ok {
		return
	}
	soft := limit / 10 * 9
	debug.SetMemoryLimit(soft)
	log.Infof("cgroup memory limit %d MiB, Go soft memory limit set to %d MiB", limit>>20, soft>>20)
}

// cgroupMemoryLimit returns the memory limit of the cgroup this process is
// in, trying cgroup v2 then v1. ok is false when there is no limit or it
// can't be read.
func cgroupMemoryLimit(procCgroup, root string) (limit int64, ok bool) {
	f, err := os.Open(procCgroup)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	var v1Path, v2Path string
	haveV2 := false
	s := bufio.NewScanner(f)
	for s.Scan() {
		// hierarchy-ID:controller-list:cgroup-path
		parts := strings.SplitN(s.Text(), ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			v2Path, haveV2 = parts[2], true
		case hasController(parts[1], "memory"):
			v1Path = parts[2]
		}
	}

	var candidates []string
	if v1Path != "" {
		// In a container the path is the host's; the container sees its own
		// cgroup at the root of the mount.
		candidates = append(candidates,
			filepath.Join(root, "memory", v1Path, "memory.limit_in_bytes"),
			filepath.Join(root, "memory", "memory.limit_in_bytes"))
	} else if haveV2 {
		candidates = append(candidates,
			filepath.Join(root, v2Path, "memory.max"),
			filepath.Join(root, "memory.max"))
	}
	for _, p := range candidates {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		v := strings.TrimSpace(string(b))
		if v == "max" {
			return 0, false
		}
		n, err := strconv.ParseInt(v, 10, 64)
		// cgroup v1 reports "no limit" as a huge page-aligned number.
		if err != nil || n <= 0 || n >= 1<<62 {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

func hasController(list, name string) bool {
	for _, c := range strings.Split(list, ",") {
		if c == name {
			return true
		}
	}
	return false
}
