package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCgroupMemoryLimit(t *testing.T) {
	tests := []struct {
		name   string
		cgroup string
		files  map[string]string
		want   int64
		wantOK bool
	}{
		{
			name:   "v2 systemd MemoryMax",
			cgroup: "0::/system.slice/v2node.service\n",
			files:  map[string]string{"system.slice/v2node.service/memory.max": "536870912\n"},
			want:   512 << 20, wantOK: true,
		},
		{
			name:   "v2 unlimited",
			cgroup: "0::/system.slice/v2node.service\n",
			files:  map[string]string{"system.slice/v2node.service/memory.max": "max\n"},
		},
		{
			name:   "v2 container sees its cgroup at the root",
			cgroup: "0::/\n",
			files:  map[string]string{"memory.max": "268435456\n"},
			want:   256 << 20, wantOK: true,
		},
		{
			name:   "v1",
			cgroup: "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n0::/\n",
			files:  map[string]string{"memory/docker/abc/memory.limit_in_bytes": "1073741824\n"},
			want:   1 << 30, wantOK: true,
		},
		{
			name:   "v1 container, host path not mounted",
			cgroup: "4:memory:/docker/abc\n",
			files:  map[string]string{"memory/memory.limit_in_bytes": "1073741824\n"},
			want:   1 << 30, wantOK: true,
		},
		{
			name:   "v1 unlimited",
			cgroup: "4:memory:/\n",
			files:  map[string]string{"memory/memory.limit_in_bytes": "9223372036854771712\n"},
		},
		{
			name:   "no cgroup files",
			cgroup: "0::/\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			proc := filepath.Join(dir, "cgroup")
			if err := os.WriteFile(proc, []byte(tt.cgroup), 0o644); err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(dir, "sys")
			for p, content := range tt.files {
				full := filepath.Join(root, p)
				os.MkdirAll(filepath.Dir(full), 0o755)
				if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			got, ok := cgroupMemoryLimit(proc, root)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("got %d, %v; want %d, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
