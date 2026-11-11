# v2node 多用戶 CPU / RAM 壓測與 pprof 報告

本報告從低配到高配，分 5 檔量測 v2node 的記憶體（RSS）、CPU 佔用和 pprof 熱點。每一檔都在同一台機器上啟動真實的 v2node 二進位檔，由 mock 面板下發用戶，再用真實的 VLESS 連線打流量。

- 測試日期：2026-11-11
- v2node：`ccr-34e7491f-26t89a` 分支（commit `1c431c2`），xray-core 為 `wyusgw/xray-core@56e6daf`（Xray 26.9.30），Go 1.27.1
- 機器：4 vCPU Intel Xeon @ 2.10GHz、16 GB RAM，Linux 6.18
- 協議：VLESS + TCP，無 TLS、無 flow（測的是代理本身的開銷，見文末「限制」）

## 測試方法

`main.go` 是壓測程式，它在同一個行程裡提供以下元件：

1. **mock 面板**：實作 `/api/v2/server/config`、`/api/v1/server/UniProxy/user` 等介面，下發 N 個用戶（uuid 固定由 id 推出）。下發的 node config 含一條 `default_out` 路由，指向一個 `finalRules` 放行 `127.0.0.1` 的 freedom outbound，因為 xray 預設會封鎖 vless 連往私有位址的流量。
2. **目標伺服器**：19000 埠是 echo，19001 埠持續送出資料（測下行吞吐）。
3. **VLESS 客戶端**：自己組 VLESS 請求頭，所以用戶數、連線數都能精確控制。

v2node 用 `taskset` 綁在指定的核上，並設好 `GOMAXPROCS` 與 `GOMEMLIMIT`（取記憶體規格的 90%，與 `cmd/memlimit.go` 在 cgroup 下的行為一致）。壓測程式綁在其餘的核上，避免兩者搶 CPU。

每一檔依序跑以下階段，每秒從 `/proc/<pid>` 取樣一次 CPU 與 RSS：

| 階段 | 內容 | pprof |
|---|---|---|
| idle | 用戶載入完成、無連線，持續 10 秒 | `heap_idle`（先 GC） |
| active | 開啟 `conns` 條長連線，每條每秒 echo 4 KB（模擬大量在線、輕度流量的用戶），持續 30 秒 | `cpu_active`（25 秒）、`heap_active`、`goroutine_active` |
| bulk | 在 active 連線之上，再開 8 或 16 條全速下載，持續 15 秒 | `cpu_bulk`（12 秒）、`heap_bulk` |
| drain | 關閉所有連線後等待 8 秒 | `heap_after`（先 GC） |

## 結果總表

| 檔位 | 用戶數 | 連線數 | v2node 核數 | idle RSS (MiB) | idle CPU % | active RSS 平均/峰值 (MiB) | active CPU % | 每連線 RSS (KiB) | goroutine | bulk RSS 峰值 (MiB) | bulk CPU % | bulk 吞吐 (Gbps) | drain RSS (MiB) | 連線錯誤 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| T1 1C/512M | 100 | 200 | 1 | 54 | 0.3 | 67 / 68 | 2.0 | 68 | 614 | 82 | 94 | 12.4 | 80 | 0 |
| T2 1C/1G | 1,000 | 1,000 | 1 | 57 | 0.4 | 118 / 121 | 9.5 | 62 | 3,014 | 176 | 94 | 12.5 | 147 | 0 |
| T3 2C/2G | 5,000 | 3,000 | 2 | 64 | 1.0 | 239 / 246 | 23.0 | 60 | 9,014 | 414 | 181 | 24.5 | 319 | 0 |
| T4 2C/4G | 20,000 | 6,000 | 2 | 87 | 2.8 | 430 / 445 | 32.4 | 59 | 18,014 | 738 | 184 | 23.6 | 539 | 0 |
| T5 3C/8G | 50,000 | 8,000 | 3 | 135 | 5.0 | 576 / 595 | 47.6 | 56 | 24,014 | 858 | 135 | 12.0* | 863 | 0 |

CPU % 以單核為 100%。每連線 RSS 的算法是 (active RSS − idle RSS) / 連線數。

\* T5 的壓測程式只分到 1 個核，bulk 吞吐受壓測端限制，不代表 v2node 的上限（v2node 只用了 135%，沒有用滿 3 核）。

各檔原始數據（含每秒取樣）在 `results/<檔位>/result.json`。

## 主要結論

### 記憶體

- **基礎開銷約 54 MiB RSS**，主要是 Go runtime 和 xray 各模組的初始化。heap 本身只有約 1 MiB，其餘是二進位檔的程式碼與資料段。
- **每個用戶約 1.1 KiB heap、約 1.6 KiB RSS**（T5 的 50,000 用戶 idle heap 約 55 MiB，RSS 比 T1 多 81 MiB）。照這個比例，10 萬用戶的閒置 RSS 約 220 MiB。用戶數本身不是瓶頸。
- **每條連線約 56–68 KiB RSS**，是記憶體的大頭。來源有三：
  - 每條連線 3 個 goroutine（goroutine 數 ≈ 3 × 連線數 + 14），每個的堆疊至少 8 KiB；
  - xray `bytespool` 的讀寫緩衝。`heap_active` 中 `bytespool.createAllocFunc` 佔 56–80%，掛在 `vless/inbound.(*Handler).Process` 和 `dispatcher.sniffer` 之下；
  - GC 預留的空間。
- **bulk 時 RSS 再上升 30–60%**：滿速連線會把每條連線的 `BufferSize`（預設 8 KB）與 pool 緩衝填滿。
- **drain 後 RSS 不會馬上回落**（例如 T5 從 576 升到 863 MiB）。`heap_after` 是在一次 GC 後抓取的，live heap 仍比 idle 多出不少（T4 118 MiB 對 idle 26 MiB，T5 212 MiB 對 55 MiB），多出的部分幾乎全是 `bytespool`（T4 82 MiB、T5 132 MiB）。這些是 `sync.Pool` 中的空閒緩衝：`sync.Pool` 的物件要經過兩次 GC 才會被丟棄（第一次只移入 victim cache），而且新流量一來就會重用。再加上 Go 不會立刻把空閒頁歸還作業系統，所以 RSS 會停在高點。這是預期行為，不是洩漏，但規劃記憶體時要以「峰值連線數」為準，而非平均值。

### CPU

- **閒置幾乎不吃 CPU**：50,000 用戶時 idle 平均 5%，峰值出現在 60 秒一次的用戶同步與流量上報。
- **大量輕流量在線連線**：每 1,000 條每秒 4 KB 的連線約佔 6–10% 單核（T2 9.5%/1k、T4 5.4%/1k、T5 6.0%/1k）。粗估 1 核可撐約 1–1.5 萬條這種連線。
- **全速轉發每核約 12 Gbps（loopback）**。`cpu_bulk` 中 84–88% 的時間在 `Syscall6`（read/writev），v2node 自己的邏輯（`CounterReader`、`ManagedWriter`、限速）只佔個位數百分比。吞吐基本隨核數線性增長（1 核 12.4 Gbps → 2 核 24.5 Gbps）。
- 在 `cpu_active` 中，`runtime.futex` 與 `netpoll`/`findRunnable` 合計約 20–35%，是大量 goroutine 喚醒與排程的成本。這是 Go 網路模型在高連線數下的正常表現。

### pprof 熱點（idle heap，T5 50,000 用戶）

| 項目 | heap | 說明 |
|---|---|---|
| `limiter.AddLimiter` | 16 MiB | 每用戶的限速與設備資料（`sync.Map` 節點佔一部分） |
| `vless.(*MemoryValidator).Add` / `ToMemoryUser` | 11 MiB | xray 內的用戶表 |
| `format.UserTag` → `fmt.Sprintf` | 9 MiB | 每用戶一個 `tag\|uuid` 字串 |
| `internal/sync` HashTrieMap 節點 | 12.5 MiB | 各處 `sync.Map` 的內部節點 |
| `GetUserList` | 5 MiB | 解碼中的用戶列表 |

5 萬用戶合計約 55 MiB，沒有異常的大物件。若要再省，`format.UserTag` 可改用字串拼接（省去 `fmt` 的開銷）；但每用戶僅約 180 B，收益很小，本次沒有改動程式碼。

## 配置建議（VLESS/TCP、無 TLS 的基準）

| 機器規格 | 建議在線連線數 | 用戶數 | 預估 RSS | 備註 |
|---|---|---|---|---|
| 1C / 512M | ≤ 2,000 | ≤ 10,000 | ~200 MiB（bulk 時 ~300 MiB） | 留給系統與 page cache 足夠空間 |
| 1C / 1G | ≤ 5,000 | ≤ 30,000 | ~400–600 MiB | CPU 會先於記憶體成為瓶頸 |
| 2C / 2G | ≤ 10,000 | ≤ 50,000 | ~700 MiB–1 GiB | |
| 2C / 4G | ≤ 20,000 | ≤ 100,000 | ~1.3–2 GiB | 需調高 `ulimit -n`（每連線 2 個 fd） |
| 4C / 8G 以上 | 30,000+ | 100,000+ | 按每連線 ~60–90 KiB 計算 | 吞吐約每核 10 Gbps 級（受網卡限制） |

以上用估算公式推算：**RSS ≈ 55 MiB + 1.6 KiB × 用戶數 + 60 KiB × 在線連線數**，滿速時連線部分再乘 1.5。

## 限制與注意事項

- **沒有 TLS/Reality**：真實節點多數使用 TLS 或 Reality，握手與加解密會讓 CPU 明顯上升，每連線的記憶體也會多出 TLS 狀態與記錄緩衝。本報告測的是代理轉發本身，TLS 的成本需要另外測。
- **loopback 吞吐**：12 Gbps/核是本機 loopback 的數字，不含網卡中斷、真實 RTT 與丟包。實際機器通常先受頻寬限制。
- **fd 上限**：本機 `ulimit -n` 為 20,000，所以最高只測到 8,000 連線（v2node 每連線 2 個 fd）。生產環境應把 `LimitNOFILE` 設為 65535 以上。
- 第一輪測試中，T4 出現 1,333 個逾時。原因是壓測程式的 echo 用了 `io.Copy(TCPConn, TCPConn)`：Go 會用 splice 實作，每條連線多佔一對 pipe fd，壓測程式自己耗盡了 fd。改成普通緩衝迴圈後，所有檔位 0 錯誤。v2node 本身每條連線始終只佔 2 個 fd。

## 重跑

```bash
test/bench/run.sh                   # 自動編譯 build_assets/v2node 再跑 5 檔
test/bench/run.sh /path/to/v2node   # 用指定的二進位檔
```

`results/` 下的 `.pb.gz` 可以直接用 pprof 查看（需要對應的二進位檔才能看到行號；只看函式名不需要）：

```bash
go tool pprof -http=:8080 test/bench/results/T4_2C4G/heap_active.pb.gz
go tool pprof -top test/bench/results/T5_3C8G/cpu_bulk.pb.gz
```

`*.top.txt` 是預先匯出的 top 列表。

單獨跑某一檔時可調整這些參數：`-users`、`-conns`、`-cpus`（v2node 綁核）、`-procs`、`-memlimit`、`-bulk`、`-hold`、`-bulksec`、`-chunk`。
