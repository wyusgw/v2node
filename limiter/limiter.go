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
	SpeedLimiter  *sync.Map      // key: TagUUID, value: *DynamicBucket
	AliveList     map[int]int    // Key: Uid, value: alive_ip
	Client        *panel.Client  // used to claim a device slot cross-node for genuinely new IPs
}

type UserLimitInfo struct {
	UID               int
	SpeedLimit        int
	DeviceLimit       int
	DynamicSpeedLimit int
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
		if users[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = users[i].SpeedLimit
		}
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
		if v, ok := l.UserLimitInfo.Load(format.UserTag(tag, modified[i].Uuid)); ok {
			u := v.(*UserLimitInfo)
			u.SpeedLimit = modified[i].SpeedLimit
			if u.DeviceLimit != modified[i].DeviceLimit {
				// Devices already admitted this report cycle won't be
				// re-checked against the limit until the cycle rolls over
				// (up to one PushInterval away). Drop this user's
				// per-cycle tracking now so the very next connection from
				// every device re-earns admission under the new limit
				// immediately instead of waiting for that rollover.
				l.clearOnlineState(format.UserTag(tag, modified[i].Uuid))
			}
			u.DeviceLimit = modified[i].DeviceLimit
			l.UserLimitInfo.Store(format.UserTag(tag, modified[i].Uuid), u)
		}
		limit := int64(determineSpeedLimit(l.SpeedLimit, modified[i].SpeedLimit)) * 1000000 / 8
		if limit > 0 {
			if v, ok := l.SpeedLimiter.Load(format.UserTag(tag, modified[i].Uuid)); ok {
				d := v.(*rate.DynamicBucket)
				d.Update(limit)
			} else {
				d := rate.NewDynamicBucket(limit)
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
		if added[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = added[i].SpeedLimit
			userLimit.ExpireTime = 0
		}
		if added[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = added[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.UserLimitInfo.Store(format.UserTag(tag, added[i].Uuid), userLimit)
		l.UUIDtoUID[added[i].Uuid] = added[i].Id
	}
}

func (l *Limiter) UpdateDynamicSpeedLimit(tag, uuid string, limit int, expire time.Time) error {
	if v, ok := l.UserLimitInfo.Load(format.UserTag(tag, uuid)); ok {
		info := v.(*UserLimitInfo)
		info.DynamicSpeedLimit = limit
		info.ExpireTime = expire.Unix()
	} else {
		return errors.New("not found")
	}
	return nil
}

func (l *Limiter) CheckLimit(ctx context.Context, taguuid string, ip string) (DynamicBucket *rate.DynamicBucket, Reject bool) {
	// check if ipv4 mapped ipv6
	ip = strings.TrimPrefix(ip, "::ffff:")

	// check and gen speed limit Bucket
	nodeLimit := l.SpeedLimit
	userLimit := 0
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		deviceLimit = u.DeviceLimit
		uid = u.UID
		if u.ExpireTime < time.Now().Unix() && u.ExpireTime != 0 {
			if u.SpeedLimit != 0 {
				userLimit = u.SpeedLimit
				u.DynamicSpeedLimit = 0
				u.ExpireTime = 0
			} else {
				l.UserLimitInfo.Delete(taguuid)
			}
		} else {
			userLimit = determineSpeedLimit(u.SpeedLimit, u.DynamicSpeedLimit)
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
	newipMap := new(sync.Map)
	newipMap.Store(ip, uid)
	// If any device is online
	if v, loaded := l.UserOnlineIP.LoadOrStore(taguuid, newipMap); loaded {
		oldipMap := v.(*sync.Map)
		// If this is a new ip this cycle
		if _, loaded := oldipMap.LoadOrStore(ip, uid); !loaded {
			if deviceLimit > 0 && !l.claimDevice(ctx, uid, ip, deviceLimit) {
				oldipMap.Delete(ip)
				return nil, true
			}
		}
	} else if deviceLimit > 0 && !l.claimDevice(ctx, uid, ip, deviceLimit) {
		l.UserOnlineIP.Delete(taguuid)
		return nil, true
	}

	limit := int64(determineSpeedLimit(nodeLimit, userLimit)) * 1000000 / 8 // If you need the Speed limit
	if limit > 0 {
		if v, ok := l.SpeedLimiter.Load(taguuid); ok {
			return v.(*rate.DynamicBucket), false
		} else {
			d := rate.NewDynamicBucket(limit)
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
