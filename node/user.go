package node

import (
	"context"
	"errors"
	"sort"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyusgw/v2node/api/v2board"
)

func (c *Controller) reportUserTrafficTask(ctx context.Context) (err error) {
	var reportmin = 0
	var devicemin = 0
	if c.info.Common.BaseConfig != nil {
		reportmin = c.info.Common.BaseConfig.NodeReportMinTraffic
		devicemin = c.info.Common.BaseConfig.DeviceOnlineMinTraffic
	}

	// AliveList is the panel's own record of which users it has seen online
	// (from GetUserAlive/alivelist), refreshed independently on the
	// nodeInfoMonitor cycle. A user never present there has never actually
	// come online through the panel, as opposed to one that's merely
	// inactive right now, so this count excludes them rather than reporting
	// every provisioned-but-unused account as part of the node's user base.
	currentUserNum := 0
	for _, u := range c.userList {
		if _, ok := c.limiter.AliveList[u.Id]; ok {
			currentUserNum++
		}
	}
	// Warn, not Info: this summary line should stay visible at the normal
	// "warning" log level instead of requiring the verbose "info" level
	// that would also surface every other per-cycle detail line below.
	log.WithField("tag", c.tag).Warnf("current user num: %d", currentUserNum)

	userTraffic, _ := c.server.GetUserTrafficSlice(c.tag, reportmin)
	if len(userTraffic) > 0 {
		err = c.apiClient.ReportUserTraffic(ctx, userTraffic)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Info("Report user traffic failed")
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		} else {
			log.WithField("tag", c.tag).Infof("Report %d users traffic", len(userTraffic))
			//log.WithField("tag", c.tag).Debugf("User traffic: %+v", userTraffic)
		}
	}

	if onlineDevice, err := c.limiter.GetOnlineDevice(); err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Info("Get online device failed")
	} else if len(*onlineDevice) > 0 {
		var result []panel.OnlineUser
		var nocountUID = make(map[int]struct{})
		for _, traffic := range userTraffic {
			total := traffic.Upload + traffic.Download
			if total < int64(devicemin*1000) {
				nocountUID[traffic.UID] = struct{}{}
			}
		}
		for _, online := range *onlineDevice {
			if _, ok := nocountUID[online.UID]; !ok {
				result = append(result, online)
			}
		}
		data := make(map[int][]string)
		for _, onlineuser := range result {
			// json structure: { UID1:["ip1","ip2"],UID2:["ip3","ip4"] }
			data[onlineuser.UID] = append(data[onlineuser.UID], onlineuser.IP)
		}
		if len(data) != 0 {
			err := c.apiClient.ReportNodeOnlineUsers(ctx, &data)
			if err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Info("Report online users failed")
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
			} else {
				onlineUID := make([]int, 0, len(data))
				for uid := range data {
					onlineUID = append(onlineUID, uid)
				}
				sort.Ints(onlineUID)
				// Warn, not Info: same as current user num above - kept
				// visible at the normal "warning" log level.
				log.WithField("tag", c.tag).Warnf("submit data success, alive user: %v", onlineUID)
				// len(result) counts one entry per (uid, ip) pair reported
				// this cycle, i.e. total online devices - distinct from
				// alive user above, which counts distinct users.
				log.WithField("tag", c.tag).Warnf("device num: %d", len(result))
			}
		}
	}

	return nil
}

func compareUserList(old, new []panel.UserInfo) (deleted, added, modified []panel.UserInfo) {
	oldMap := make(map[string]panel.UserInfo, len(old))
	for _, u := range old {
		oldMap[u.Uuid] = u
	}

	for _, u := range new {
		if o, ok := oldMap[u.Uuid]; !ok {
			added = append(added, u)
		} else {
			if o.SpeedLimit != u.SpeedLimit || o.DeviceLimit != u.DeviceLimit {
				modified = append(modified, u)
			}
			delete(oldMap, u.Uuid)
		}
	}

	for _, o := range oldMap {
		deleted = append(deleted, o)
	}

	return deleted, added, modified
}
