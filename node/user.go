package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyusgw/v2node/api/v2board"
)

// logSummary writes a compact "timestamp\tLEVEL\tmessage" status line
// directly to the same output the rest of the app logs to (respecting
// -o/Log.Output), instead of the tag=/err= key=value fields logrus's
// TextFormatter attaches to every other log call. These three lines are
// meant to be read as an at-a-glance status feed, not troubleshooting
// detail, so they're deliberately logged at Warn severity - that's what
// keeps them visible at the normal "warning" Log.Level instead of
// requiring "info" - while still displaying as "INFO" since that's what
// they semantically are. minLevel gates them the same way any other log
// call is gated by the configured level.
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

	// c.userList is the panel's /user response for this node, which only
	// ever contains users with a currently valid subscription assigned to
	// it - not a locally-tracked online-history set, so this is the count
	// of currently-valid-subscription users regardless of whether they've
	// ever connected.
	logSummary(log.WarnLevel, "current user num: %d", len(c.userList))

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
