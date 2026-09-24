# v2node 多用戶 CPU / 記憶體基準測試（低→高配置，附 pprof）

本目錄是可重現的壓測工具，以及一次完整測量的結果（`results/`）。每次測量都附 pprof：`cpu.pprof`、`heap.pprof`，和用 `go tool pprof -top` 生成的 `cpu_top.txt`、`heap_top.txt`、`alloc_top.txt`。

## 怎麼測的

| 元件 | 作用 |
|---|---|
| `mockpanel/` | 模擬 v2board UniProxy API：下發節點配置和 N 個用戶，接收流量／在線上報 |
| `loadgen/` | 用 xray-core 自己的客戶端編碼（vless / vmess / trojan / shadowsocks）建立大量併發連線，每條連線隨機選一個用戶，按固定速率下載 32 KB/s、上傳 4 KB/s；平均 20 秒斷開，再換一個隨機用戶重連（持續產生握手） |
| `run.sh` | 用 `taskset` 把 v2node 綁到指定核數，放進記憶體 cgroup（超出就觸發 OOM），同時採樣 CPU 和 RSS，並抓 pprof |

- 機器：4 vCPU / 16 GB，Linux 6.18，Go 1.26.1，v2node `bbe2977` + xray-core `b6ee6fb`
- 節點：預設 ConnectionConfig（BufferSize 8 KB），TCP，無 TLS，sniffing 開啟（v2node 預設）
- 配置檔位（給 v2node 的資源）：**1C/512M → 1C/1G → 2C/2G → 3C/4G**；loadgen 和面板跑在剩下的核上
- 首包是 HTTP 請求行，sniffer 能立刻識別。無法識別的首包會讓 sniffer 等最多約 100 ms，會扭曲延遲數據。
- 目標位址 `11.11.11.11` 由 iptables DNAT 轉到本機 sink，因為 xray freedom 預設拒絕私有位址

重跑：`sudo bench/run.sh`（完整約 45 分鐘），或者縮小範圍：`PROTOCOLS=vless TIERS="1:512" LOADS=500 bench/run.sh`

## 結果一：用戶數對記憶體的影響（無流量，1C/512M）

`rss_anon` 是去掉映射的二進位檔（約 50 MB 的共享頁）之後的 RSS，比較接近真實的記憶體佔用。

| 協議 | 1k 用戶 | 10k | 50k | 100k | 每增加 1 個用戶 (anon) | 啟動時間 @100k |
|---|---|---|---|---|---|---|
| vless | 8 MB | 25 MB | 85 MB | 168 MB | ≈ 1.6 KB | 0.6 s |
| vmess | 10 MB | 32 MB | 121 MB | 240 MB | ≈ 2.3 KB | 0.6 s |
| trojan | 11 MB | 29 MB | 117 MB | 275 MB | ≈ 2.7 KB | 0.6 s |
| shadowsocks | 8 MB | 18 MB | 64 MB | 121 MB | ≈ 1.1 KB | 0.7 s |

所有協議空載 CPU 都是 0%，goroutine 數固定（12–15），不隨用戶數增長。10 萬用戶在 512 MB 上都能跑。

每個用戶的記憶體主要花在哪裡（`results/idle/*/u100000/heap_top.txt`）：
- 用戶的 email tag 字串 `fmt.Sprintf("%s|%s")`：100k 用戶約 23–27 MB
- `limiter.AddLimiter`：每個用戶的限速資訊，約 10–12 MB
- 各協議自己的驗證表：vmess 每個用戶一個 AES cipher（`aes.New` 38 MB）；trojan 保存 sha224 的 hex 字串，外加 sync.Map 節點（約 50 MB）

## 結果二：固定負載下的 CPU / 記憶體（10,000 用戶）

每條連線 32 KB/s 下行，所以 500 / 2000 / 5000 連線對應 131 / 524 / 1311 Mbps 的目標流量。
`CPU%` 以單核為 100%。`anon` 是穩態時的 RssAnon。`TTFB p50/p99` 是從發起連線到收到第一個位元組的時間。

### vless
| 檔位 | 500 連線 | 2000 連線 | 5000 連線 |
|---|---|---|---|
| 1C/512M | 23% · 63 MB · 0.7/5.5 ms | 76% · 176 MB · 1.3/76 ms | **98%** · 409 MB · 330/672 ms（接近飽和） |
| 1C/1G | 23% · 62 MB | 74% · 178 MB | 98% · 411 MB · 321/582 ms |
| 2C/2G | 28% · 64 MB · 0.6/2.9 ms | 89% · 180 MB · 0.6/17 ms | 165% · 415 MB · 53/295 ms，跑滿 1299 Mbps |
| 3C/4G | 34% · 66 MB | 109% · 177 MB · 1.0/35 ms | ⚠ 瓶頸在 loadgen（只剩 1 核） |

### vmess
| 檔位 | 500 | 2000 | 5000 |
|---|---|---|---|
| 1C/512M | 38% · 91 MB · 5.6/44 ms | **98%** · 238 MB · 226/574 ms | ❌ **OOM**（CPU 飽和，排隊的連線把記憶體撐爆） |
| 1C/1G | 38% · 90 MB | 97% · 242 MB · 212/651 ms | ❌ **OOM**，1 GB 也不夠 |
| 2C/2G | 51% · 93 MB · 4.1/29 ms | 137% · 236 MB · 7/218 ms | 187% · 567 MB · 只跑到 1118/1311 Mbps，TTFB 1.1 s |
| 3C/4G | 54% · 94 MB | 159% · 240 MB · 10/178 ms | ⚠ 瓶頸在 loadgen |

### trojan
| 檔位 | 500 | 2000 | 5000 |
|---|---|---|---|
| 1C/512M | 27% · 80 MB · 0.9/7.7 ms | 83% · 246 MB · 1.9/147 ms | ❌ **OOM** |
| 1C/1G | 27% · 80 MB | 84% · 244 MB | 98% · 566 MB · 442/958 ms（1G 下沒有 OOM） |
| 2C/2G | 33% · 81 MB · 0.7/3.6 ms | 99% · 229 MB · 0.7/11 ms | 171% · 576 MB · 163/417 ms，跑滿 1293 Mbps |
| 3C/4G | 41% · 86 MB | 129% · 233 MB | ⚠ 瓶頸在 loadgen |

### shadowsocks（aes-128-gcm，多用戶）
| 檔位 | 500 | 2000 | 5000 |
|---|---|---|---|
| 1C/512M | **84%** · 92 MB · **327/2407 ms** | ❌ 99%，只跑到 111/524 Mbps，大量 IO 錯誤 | ❌ **OOM** |
| 1C/1G | 86% · 88 MB · 360/2689 ms | ❌ 99%，129/524 Mbps | ❌ 99%，134/1311 Mbps |
| 2C/2G | 89% · 104 MB · 25/99 ms | ❌ 196%，233/524 Mbps | ❌ 197%，214/1311 Mbps |
| 3C/4G | 96% · 106 MB · 18/58 ms | ❌ 292%，349/524 Mbps | ❌ |

完整數據：`results/load.csv`、`results/idle.csv`。

⚠ 3C/4G 檔位只剩 1 核給 loadgen，5000 連線那一列的吞吐量比 2C 還低，所以那一列測到的是 loadgen 的上限，不是 v2node 的。500 和 2000 連線的數據都有效（吞吐量達標）。

## 配置建議（10k 用戶，連線平均 32 KB/s）

| 檔位 | vless / trojan | vmess | shadowsocks（多用戶 AEAD） |
|---|---|---|---|
| 1C/512M | ≤ 2000 連線（約 500 Mbps） | ≤ 1500 連線 | ≤ 500 連線，而且握手已有 0.3–2.4 s 延遲，**不建議** |
| 1C/1G | 同上；多出來的記憶體只能避免 trojan 過載時 OOM | 同上 | 同上 |
| 2C/2G | 約 5000 連線 / 1.3 Gbps | ≤ 3000 連線 | ≤ 500 連線 |
| 3C/4G | > 5000 連線（推算） | 約 5000 連線（推算） | ≈ 1000 連線（推算） |

粗略換算：每 100 Mbps 大約需要 vless 15%、trojan 16%、vmess 25–29% 的單核 CPU。每條活躍連線大約佔 75–110 KB 記憶體。

## pprof 看出的熱點與優化方向

1. **Shadowsocks 多用戶握手是 O(在線用戶數)，是最大的瓶頸。** 只有 25 次握手/秒時，`Validator.GetWithCache` 就佔了 47% 的 CPU，其中 `hkdfSHA1` 佔 30%，剩下大部分是由此產生的 GC（`results/load/shadowsocks/1c512m/c500/cpu_top.txt`）。原因是每個新連線都要拿每個候選用戶的密鑰試一遍 HKDF-SHA1 + AEAD。
   - xray-core `proxy/shadowsocks/server.go:213` 用 `conn.RemoteAddr().String()` 做第一級「同 IP」快取的 key，這個字串**包含來源端口**。TCP 每個新連線的端口都不一樣，所以第一級快取實際上幾乎不會命中，每次都退回到遍歷所有成功過的用戶。改成只用 IP 做 key，能大幅減少生產環境的握手開銷。（本次壓測所有連線都來自 127.0.0.1，會觸發「中轉」判定，所以測不出這項改動的效果，本次沒有修改。）
   - 用戶多的節點建議改用 SS2022（`server_key`，按 EIH 直接查用戶），或者 vless / trojan。
2. **VMess 的 `AuthIDDecoderHolder.Match` 佔 22% CPU**（2C、5000 連線）。每次握手都要用 AES 逐個試用戶，而且每個用戶常駐一個 AES cipher。這是 vmess 協議本身的代價，所以同樣配置下 vmess 能承載的連線數比 vless 少。
3. **CPU 飽和時記憶體會失控。** vmess 在 1 核上遇到 5000 連線，連 1 GB 都 OOM：不是洩漏，是處理不過來的連線持續堆積。可以考慮設 `GOMEMLIMIT` 或限制併發握手數，讓過載時優雅降級而不是被 OOM kill。
4. **負載下的記憶體主要是連線緩衝區：** `bytespool` 在 5000 連線時佔 96–109 MB heap，每條連線大約 3 個 goroutine（加上 vmess 的 cipher，約 6 個）。BufferSize 已經是 8 KB，再調小的收益有限。
5. **vless / trojan 的 CPU 大約 65% 花在系統調用**（每條連線每 100 ms 只收發幾 KB 的小包），用戶態本身已經很輕。
6. 空載時每個用戶的記憶體中，email tag 字串、limiter 和 sync.Map 佔大頭（見結果一），10 萬用戶合計約 30–50 MB，可以進一步壓縮，但優先級低。

## 目錄結構

```
results/
  idle.csv, load.csv
  idle/<協議>/u<用戶數>/         heap.pprof, heap_top.txt, alloc_top.txt, node.log
  load/<協議>/<核>c<MB>m/c<連線>/ cpu.pprof, heap.pprof, *_top.txt, loadgen.json, node.log
```

pprof 檔本身帶有符號，可以直接打開，例如：
`go tool pprof -http=:8080 results/load/vmess/2c2048m/c5000/cpu.pprof`
