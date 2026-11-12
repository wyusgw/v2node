package limiter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	UserOnlineIP  *sync.Map      // Key: TagUUID, value: {Key: Ip, value: Uid}
	UUIDtoUID     map[string]int // Key: UUID, value: Uid
	UserLimitInfo *sync.Map      // Key: TagUUID value: UserLimitInfo
	SpeedLimiter  *sync.Map      // key: BucketKey, value: *rate.DuplexBucket
	AliveList     map[int]int    // Key: Uid, value: alive_ip
	Kicked        *sync.Map      // Key: "Uid|Ip", value: unix time the kick block ends
	Client        *panel.Client  // used to claim a device slot cross-node for genuinely new IPs
}

type UserLimitInfo struct {
	UID          int
	SpeedLimit   int64 // download, bytes/s
	SpeedLimitUp int64 // upload, bytes/s
	BucketKey    string
	DeviceLimit  int
	OverLimit    bool
}

// BucketKey returns the SpeedLimiter key of u, "" when u is unlimited. Users
// sharing a plan's limit share one bucket per limit value, so a user given a
// different limit than the rest of the plan isn't throttled by their bucket.
func BucketKey(tag string, u panel.UserInfo) string {
	up := u.SpeedLimitUpBytes()
	down := u.SpeedLimitBytes()
	if up <= 0 && down <= 0 {
		return ""
	}
	if u.SpeedLimitGroup > 0 {
		return fmt.Sprintf("%s|#%d|%d|%d", tag, u.SpeedLimitGroup, up, down)
	}
	return format.UserTag(tag, u.Uuid)
}

func isSharedKey(tag string, key string) bool {
	return strings.HasPrefix(key, tag+"|#")
}

func AddLimiter(nodetype string, tag string, users []panel.UserInfo, aliveList map[int]int, client *panel.Client) *Limiter {
	l := &Limiter{
		Nodetype:      nodetype,
		UserOnlineIP:  new(sync.Map),
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
		AliveList:     aliveList,
		Kicked:        new(sync.Map),
		Client:        client,
	}
	uuidmap := make(map[string]int)
	for i := range users {
		uuidmap[users[i].Uuid] = users[i].Id
		userLimit := &UserLimitInfo{}
		userLimit.UID = users[i].Id
		userLimit.SpeedLimit = users[i].SpeedLimitBytes()
		userLimit.SpeedLimitUp = users[i].SpeedLimitUpBytes()
		userLimit.BucketKey = BucketKey(tag, users[i])
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
	prune := false
	for i := range deleted {
		if deleted[i].SpeedLimitGroup > 0 {
			prune = true
		}
		l.UserLimitInfo.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.UserOnlineIP.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.SpeedLimiter.Delete(format.UserTag(tag, deleted[i].Uuid))
		delete(l.UUIDtoUID, deleted[i].Uuid)
		delete(l.AliveList, deleted[i].Id)
	}
	for i := range modified {
		key := format.UserTag(tag, modified[i].Uuid)
		deviceLimitChanged := false
		bucketKey := BucketKey(tag, modified[i])
		l.updateInfo(key, func(u *UserLimitInfo) {
			if isSharedKey(tag, u.BucketKey) && u.BucketKey != bucketKey {
				prune = true
			}
			u.SpeedLimit = modified[i].SpeedLimitBytes()
			u.SpeedLimitUp = modified[i].SpeedLimitUpBytes()
			u.BucketKey = bucketKey
			deviceLimitChanged = u.DeviceLimit != modified[i].DeviceLimit
			u.DeviceLimit = modified[i].DeviceLimit
		})
		if deviceLimitChanged {
			// Admitted devices aren't re-checked until the report cycle rolls over, so drop
			// this user's tracking to apply the new limit on their next connection.
			l.clearOnlineState(key)
		}
		up := modified[i].SpeedLimitUpBytes()
		down := modified[i].SpeedLimitBytes()
		if bucketKey == key {
			if v, ok := l.SpeedLimiter.Load(key); ok {
				v.(*rate.DuplexBucket).Update(up, down)
			} else {
				// LoadOrStore: a bucket CheckLimit creates meanwhile must not be replaced.
				v, loaded := l.SpeedLimiter.LoadOrStore(key, rate.NewDuplexBucket(up, down))
				if loaded {
					v.(*rate.DuplexBucket).Update(up, down)
				}
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
		userLimit.BucketKey = BucketKey(tag, added[i])
		if added[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = added[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.UserLimitInfo.Store(format.UserTag(tag, added[i].Uuid), userLimit)
		l.UUIDtoUID[added[i].Uuid] = added[i].Id
	}
	if prune {
		l.pruneSharedBuckets(tag)
	}
}

// pruneSharedBuckets drops the shared buckets no user of tag maps to anymore.
func (l *Limiter) pruneSharedBuckets(tag string) {
	used := make(map[string]struct{})
	l.UserLimitInfo.Range(func(_, value interface{}) bool {
		k := value.(*UserLimitInfo).BucketKey
		if isSharedKey(tag, k) {
			used[k] = struct{}{}
		}
		return true
	})
	l.SpeedLimiter.Range(func(key, _ interface{}) bool {
		k := key.(string)
		if _, ok := used[k]; !ok && isSharedKey(tag, k) {
			l.SpeedLimiter.Delete(k)
		}
		return true
	})
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

	var down, up int64
	var bucketKey string
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		deviceLimit = u.DeviceLimit
		uid = u.UID
		down = u.SpeedLimit
		up = u.SpeedLimitUp
		bucketKey = u.BucketKey
	} else {
		return nil, true
	}
	if l.isKicked(uid, ip) {
		return nil, true
	}
	// Applies to UDP-only devices too. UserOnlineIP is wiped every report cycle, so each ip
	// goes through claimDevice at least once per cycle: the panel's claim entry would otherwise
	// expire while the device is connected and free its slot for another device.
	// An ip is stored only after the claim succeeds, since GetOnlineDevice snapshots this map
	// concurrently and could otherwise report a device that is about to be rejected.
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

	if bucketKey != "" {
		if v, ok := l.SpeedLimiter.Load(bucketKey); ok {
			return v.(*rate.DuplexBucket), false
		}
		// Concurrent first connections of a user must share one bucket, or
		// each would get the full rate.
		v, _ := l.SpeedLimiter.LoadOrStore(bucketKey, rate.NewDuplexBucket(up, down))
		return v.(*rate.DuplexBucket), false
	} else {
		return nil, false
	}
}

func kickKey(uid int, ip string) string {
	return strconv.Itoa(uid) + "|" + ip
}

func (l *Limiter) isKicked(uid int, ip string) bool {
	v, ok := l.Kicked.Load(kickKey(uid, ip))
	return ok && v.(int64) > time.Now().Unix()
}

// ApplyKicks blocks the ips the panel kicked offline until their block ends,
// and drops them from this cycle's admitted set so they aren't reported as
// online again. Expired blocks are cleared here as well.
func (l *Limiter) ApplyKicks(kicks map[int]map[string]int) {
	now := time.Now().Unix()
	l.Kicked.Range(func(key, value interface{}) bool {
		if value.(int64) <= now {
			l.Kicked.Delete(key)
		}
		return true
	})
	if len(kicks) == 0 {
		return
	}
	for uid, ips := range kicks {
		for ip, seconds := range ips {
			l.Kicked.Store(kickKey(uid, ip), now+int64(seconds))
		}
	}
	l.UserOnlineIP.Range(func(_, value interface{}) bool {
		ipMap := value.(*sync.Map)
		ipMap.Range(func(key, value interface{}) bool {
			if ips, ok := kicks[value.(int)]; ok {
				if _, ok := ips[key.(string)]; ok {
					ipMap.Delete(key)
				}
			}
			return true
		})
		return true
	})
}

// clearOnlineState drops taguuid's per-cycle admitted-IP set, so the next
// connection from every device this user currently has open is treated as
// new-this-cycle and goes through claimDevice again immediately.
func (l *Limiter) clearOnlineState(taguuid string) {
	l.UserOnlineIP.Delete(taguuid)
}

// claimDevice asks the panel to atomically check and register ip against uid's device limit.
// A nil client means no limit configured and allows; any request error rejects (fail-closed).
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

// UID returns the panel user id of taguuid.
func (l *Limiter) UID(taguuid string) (int, bool) {
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		return 0, false
	}
	return v.(*UserLimitInfo).UID, true
}

// Reclaim asks the panel again for the slot of a device of taguuid that is
// already connected, after its device limit changed, and reports whether the
// panel refused it. A failed request is not a refusal: it must not drop
// devices that hold their slot.
func (l *Limiter) Reclaim(ctx context.Context, taguuid string, ip string) bool {
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok || l.Client == nil {
		return false
	}
	u := v.(*UserLimitInfo)
	if u.DeviceLimit <= 0 {
		return false
	}
	cctx, cancel := context.WithTimeout(ctx, claimDeviceTimeout)
	defer cancel()
	allow, err := l.Client.ClaimDevice(cctx, u.UID, ip, u.DeviceLimit)
	return err == nil && !allow
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
