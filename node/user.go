package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyusgw/v2node/api/v2board"
)

// logSummary writes a compact "timestamp\tLEVEL\tmessage" status line to the app's log output,
// without logrus's key=value fields. The lines are logged at Warn severity to stay visible at
// the normal Log.Level, but display as INFO; minLevel gates them like any other log call.
func logSummary(minLevel log.Level, format string, args ...interface{}) {
	if log.GetLevel() < minLevel {
		return
	}
	fmt.Fprintf(log.StandardLogger().Out, "%s\tINFO\t%s\n",
		time.Now().Format("2006/01/02 15:04:05"), fmt.Sprintf(format, args...))
}

func (c *Controller) reportUserTrafficTask(ctx context.Context) (err error) {
	var reportmin = 0
	var devicemin = 0
	if c.info.Common.BaseConfig != nil {
		reportmin = c.info.Common.BaseConfig.NodeReportMinTraffic
		devicemin = c.info.Common.BaseConfig.DeviceOnlineMinTraffic
	}

	// c.userList only holds users with a valid subscription on this node, so this counts them
	// whether or not they have ever connected.
	logSummary(log.WarnLevel, "current user num: %d", c.userNum.Load())

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
	} else if devices := c.addOpenLinkDevices(*onlineDevice); len(devices) > 0 {
		var result []panel.OnlineUser
		var nocountUID = make(map[int]struct{})
		for _, traffic := range userTraffic {
			total := traffic.Upload + traffic.Download
			if total < int64(devicemin*1000) {
				nocountUID[traffic.UID] = struct{}{}
			}
		}
		for _, online := range devices {
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
				logSummary(log.WarnLevel, "submit data success, alive user: %d", len(data))
				// len(result) counts one entry per (uid, ip) pair reported
				// this cycle, i.e. total online devices - distinct from
				// alive user above, which counts distinct users.
				logSummary(log.WarnLevel, "device num: %d", len(result))
			}
		}
	}

	return nil
}

// addOpenLinkDevices adds the devices that still have connections open but
// opened none this cycle, so a device on one long connection stays reported
// online and keeps its slot in the panel.
func (c *Controller) addOpenLinkDevices(devices []panel.OnlineUser) []panel.OnlineUser {
	seen := make(map[panel.OnlineUser]struct{}, len(devices))
	for _, d := range devices {
		seen[d] = struct{}{}
	}
	for email, ips := range c.server.OpenLinkIPs(c.tag) {
		uid, ok := c.limiter.UID(email)
		if !ok {
			continue
		}
		for _, ip := range ips {
			d := panel.OnlineUser{UID: uid, IP: ip}
			if _, ok := seen[d]; !ok {
				seen[d] = struct{}{}
				devices = append(devices, d)
			}
		}
	}
	return devices
}

func (c *Controller) reportBehaviorLogTask(ctx context.Context) (err error) {
	records, _ := c.server.GetBehaviorSlice(c.tag)
	if len(records) == 0 {
		return nil
	}
	err = c.apiClient.ReportBehaviorLog(ctx, records)
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Info("Report behavior log failed")
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return nil
	}
	log.WithField("tag", c.tag).Infof("Report %d behavior log records", len(records))
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
			if o.SpeedLimitBytes() != u.SpeedLimitBytes() || o.SpeedLimitUpBytes() != u.SpeedLimitUpBytes() || o.DeviceLimit != u.DeviceLimit || o.SpeedLimitGroup != u.SpeedLimitGroup {
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
