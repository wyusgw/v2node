package limiter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	panel "github.com/wyusgw/v2node/api/v2board"
	"github.com/wyusgw/v2node/common/format"
	"github.com/wyusgw/v2node/common/rate"
)

var limitLock sync.RWMutex
var limiter map[string]*Limiter

// claimDeviceTimeout bounds how long a brand-new device's first connection
// on a node can be held up waiting on the panel's cross-node device-claim
// check, so a slow/unreachable panel delays new connections by a fixed,
// short amount rather than the client's full request timeout/retry budget.
const claimDeviceTimeout = 3 * time.Second

func Init() {
	limiter = map[string]*Limiter{}
}

type Limiter struct {
	Nodetype      string         // Node type, e.g. "v2ray", "trojan", "shadowsocks"
	SpeedLimit    int            // Node speed limit in Mbps
	UserOnlineIP  *sync.Map      // Key: TagUUID, value: {Key: Ip, value: Uid}
	UUIDtoUID     map[string]int // Key: UUID, value: Uid
	UserLimitInfo *sync.Map      // Key: TagUUID value: UserLimitInfo
	SpeedLimiter  *sync.Map      // key: TagUUID, value: *rate.DuplexBucket
	AliveList     map[int]int    // Key: Uid, value: alive_ip
	Client        *panel.Client  // used to claim a device slot cross-node for genuinely new IPs
}

type UserLimitInfo struct {
	UID               int
	SpeedLimit        int64 // download, bytes/s
	SpeedLimitUp      int64 // upload, bytes/s
	DeviceLimit       int
	DynamicSpeedLimit int64 // bytes/s
	ExpireTime        int64
	OverLimit         bool
}

func AddLimiter(nodetype string, tag string, users []panel.UserInfo, aliveList map[int]int, client *panel.Client) *Limiter {
	l := &Limiter{
		Nodetype:      nodetype,
		UserOnlineIP:  new(sync.Map),
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
		AliveList:     aliveList,
		Client:        client,
	}
	uuidmap := make(map[string]int)
	for i := range users {
		uuidmap[users[i].Uuid] = users[i].Id
		userLimit := &UserLimitInfo{}
		userLimit.UID = users[i].Id
		userLimit.SpeedLimit = users[i].SpeedLimitBytes()
		userLimit.SpeedLimitUp = users[i].SpeedLimitUpBytes()
		if users[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = users[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.UserLimitInfo.Store(format.UserTag(tag, users[i].Uuid), userLimit)
	}
	l.UUIDtoUID = uuidmap
	limitLock.Lock()
	limiter[tag] = l
	limitLock.Unlock()
	return l
}

func GetLimiter(tag string) (info *Limiter, err error) {
	limitLock.RLock()
	info, ok := limiter[tag]
	limitLock.RUnlock()
	if !ok {
		return nil, errors.New("not found")
	}
	return info, nil
}

func DeleteLimiter(tag string) {
	limitLock.Lock()
	delete(limiter, tag)
	limitLock.Unlock()
}

func (l *Limiter) UpdateUser(tag string, added []panel.UserInfo, deleted []panel.UserInfo, modified []panel.UserInfo) {
	for i := range deleted {
		l.UserLimitInfo.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.UserOnlineIP.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.SpeedLimiter.Delete(format.UserTag(tag, deleted[i].Uuid))
		delete(l.UUIDtoUID, deleted[i].Uuid)
		delete(l.AliveList, deleted[i].Id)
	}
	for i := range modified {
		key := format.UserTag(tag, modified[i].Uuid)
		deviceLimitChanged := false
		l.updateInfo(key, func(u *UserLimitInfo) {
			u.SpeedLimit = modified[i].SpeedLimitBytes()
			u.SpeedLimitUp = modified[i].SpeedLimitUpBytes()
			deviceLimitChanged = u.DeviceLimit != modified[i].DeviceLimit
			u.DeviceLimit = modified[i].DeviceLimit
		})
		if deviceLimitChanged {
			// Devices already admitted this report cycle won't be
			// re-checked against the limit until the cycle rolls over
			// (up to one PushInterval away). Drop this user's
			// per-cycle tracking now so the very next connection from
			// every device re-earns admission under the new limit
			// immediately instead of waiting for that rollover.
			l.clearOnlineState(key)
		}
		nodeLimit := panel.MbpsToBytes(l.SpeedLimit)
		up := determineSpeedLimit(nodeLimit, modified[i].SpeedLimitUpBytes())
		down := determineSpeedLimit(nodeLimit, modified[i].SpeedLimitBytes())
		if up > 0 || down > 0 {
			if v, ok := l.SpeedLimiter.Load(format.UserTag(tag, modified[i].Uuid)); ok {
				d := v.(*rate.DuplexBucket)
				d.Update(up, down)
			} else {
				d := rate.NewDuplexBucket(up, down)
				l.SpeedLimiter.Store(format.UserTag(tag, modified[i].Uuid), d)
			}
		} else {
			l.SpeedLimiter.Delete(format.UserTag(tag, modified[i].Uuid))
		}
	}
	for i := range added {
		userLimit := &UserLimitInfo{
			UID: added[i].Id,
		}
		userLimit.SpeedLimit = added[i].SpeedLimitBytes()
		userLimit.SpeedLimitUp = added[i].SpeedLimitUpBytes()
		if added[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = added[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.UserLimitInfo.Store(format.UserTag(tag, added[i].Uuid), userLimit)
		l.UUIDtoUID[added[i].Uuid] = added[i].Id
	}
}

func (l *Limiter) UpdateDynamicSpeedLimit(tag, uuid string, limit int, expire time.Time) error {
	if !l.updateInfo(format.UserTag(tag, uuid), func(u *UserLimitInfo) {
		u.DynamicSpeedLimit = panel.MbpsToBytes(limit)
		u.ExpireTime = expire.Unix()
	}) {
		return errors.New("not found")
	}
	return nil
}

// updateInfo applies fn to a copy of key's UserLimitInfo and publishes the
// copy. CheckLimit reads these entries without a lock on every new
// connection, so they are never modified in place.
func (l *Limiter) updateInfo(key string, fn func(*UserLimitInfo)) bool {
	for {
		v, ok := l.UserLimitInfo.Load(key)
		if !ok {
			return false
		}
		old := v.(*UserLimitInfo)
		u := *old
		fn(&u)
		if l.UserLimitInfo.CompareAndSwap(key, old, &u) {
			return true
		}
	}
}

func (l *Limiter) CheckLimit(ctx context.Context, taguuid string, ip string) (Bucket *rate.DuplexBucket, Reject bool) {
	// check if ipv4 mapped ipv6
	ip = strings.TrimPrefix(ip, "::ffff:")

	// check and gen speed limit Bucket
	nodeLimit := panel.MbpsToBytes(l.SpeedLimit)
	var userLimit, userLimitUp int64
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		deviceLimit = u.DeviceLimit
		uid = u.UID
		if u.ExpireTime < time.Now().Unix() && u.ExpireTime != 0 {
			// The dynamic limit has expired: fall back to the user's own
			// limits. The entry must stay - a missing entry means "unknown
			// user" and every later connection would be rejected.
			userLimit = u.SpeedLimit
			userLimitUp = u.SpeedLimitUp
			expired := *u
			expired.DynamicSpeedLimit = 0
			expired.ExpireTime = 0
			// Lose to a concurrent update rather than overwrite it.
			l.UserLimitInfo.CompareAndSwap(taguuid, u, &expired)
		} else {
			userLimit = determineSpeedLimit(u.SpeedLimit, u.DynamicSpeedLimit)
			userLimitUp = determineSpeedLimit(u.SpeedLimitUp, u.DynamicSpeedLimit)
		}
	} else {
		return nil, true
	}
	// Store online user for device limit. This runs for every inbound
	// regardless of TCP/UDP: a device whose only traffic to this node is UDP
	// (e.g. DNS-only, or a protocol other than hysteria2/tuic that never
	// opens a TCP connection here) used to skip this whole block and so was
	// never subject to the device limit at all.
	//
	// UserOnlineIP only dedups within the current report cycle (it's wiped
	// by GetOnlineDevice() every cycle): the first connection this cycle
	// from a given ip always goes through claimDevice, including for a
	// device that's been continuously online for a while. That's
	// deliberate, not just an optimization boundary - the panel's
	// Redis-backed claim entry for that ip has to be re-confirmed at least
	// once per cycle, or it ages out of the shared claim set (device_claim
	// TTL) while the device is still connected, freeing its slot for a
	// different device to claim and letting the user exceed deviceLimit in
	// aggregate even though no single node ever saw too many at once.
	//
	// An ip is only ever written into UserOnlineIP after claimDevice has
	// confirmed it (or immediately, when there's no limit to confirm against).
	// GetOnlineDevice() - the periodic report to the panel - runs
	// concurrently on its own timer and takes a live snapshot of this map.
	// claimDevice's HTTP round trip can take up to claimDeviceTimeout, so
	// writing the ip optimistically and deleting it again on rejection would
	// leave a window where a report snapshot lands between the write and the
	// delete and wrongly tells the panel a device is online right before
	// it's rejected.
	var ipMap *sync.Map
	if v, ok := l.UserOnlineIP.Load(taguuid); ok {
		ipMap = v.(*sync.Map)
	}
	isNewIPThisCycle := true
	if ipMap != nil {
		if _, ok := ipMap.Load(ip); ok {
			isNewIPThisCycle = false
		}
	}
	if isNewIPThisCycle && deviceLimit > 0 && !l.claimDevice(ctx, uid, ip, deviceLimit) {
		return nil, true
	}
	if ipMap == nil {
		newIPMap := new(sync.Map)
		if v, loaded := l.UserOnlineIP.LoadOrStore(taguuid, newIPMap); loaded {
			ipMap = v.(*sync.Map)
		} else {
			ipMap = newIPMap
		}
	}
	ipMap.Store(ip, uid)

	down := determineSpeedLimit(nodeLimit, userLimit)
	up := determineSpeedLimit(nodeLimit, userLimitUp)
	if up > 0 || down > 0 {
		if v, ok := l.SpeedLimiter.Load(taguuid); ok {
			return v.(*rate.DuplexBucket), false
		} else {
			d := rate.NewDuplexBucket(up, down)
			l.SpeedLimiter.Store(taguuid, d)
			return d, false
		}
	} else {
		return nil, false
	}
}

// clearOnlineState drops taguuid's per-cycle admitted-IP set, so the next
// connection from every device this user currently has open is treated as
// new-this-cycle and goes through claimDevice again immediately.
func (l *Limiter) clearOnlineState(taguuid string) {
	l.UserOnlineIP.Delete(taguuid)
}

// claimDevice asks the panel to atomically check ip against uid's device
// limit in its cross-node online-IP set and register it if admitted. It
// fails closed: a nil client (gating not wired up) is treated as "no limit
// configured" and allowed, but any request error (panel or Redis
// unreachable, timeout, bad response) is treated as a reject rather than
// letting an unverifiable device through.
func (l *Limiter) claimDevice(ctx context.Context, uid int, ip string, deviceLimit int) bool {
	if l.Client == nil {
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, claimDeviceTimeout)
	defer cancel()
	allow, err := l.Client.ClaimDevice(cctx, uid, ip, deviceLimit)
	if err != nil {
		return false
	}
	return allow
}

func (l *Limiter) GetOnlineDevice() (*[]panel.OnlineUser, error) {
	var onlineUser []panel.OnlineUser
	l.UserOnlineIP.Range(func(key, value interface{}) bool {
		taguuid := key.(string)
		ipMap := value.(*sync.Map)
		ipMap.Range(func(key, value interface{}) bool {
			uid := value.(int)
			ip := key.(string)
			onlineUser = append(onlineUser, panel.OnlineUser{UID: uid, IP: ip})
			return true
		})
		l.UserOnlineIP.Delete(taguuid) // Reset online device, forcing every device through claimDevice again next cycle
		return true
	})

	return &onlineUser, nil
}

type UserIpList struct {
	Uid    int      `json:"Uid"`
	IpList []string `json:"Ips"`
}
