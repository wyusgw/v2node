package node

import (
	"context"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyusgw/v2node/api/v2board"
	"github.com/wyusgw/v2node/common/format"
	"github.com/wyusgw/v2node/common/task"
	vCore "github.com/wyusgw/v2node/core"
	"github.com/wyusgw/v2node/limiter"
)

func (c *Controller) startTasks(node *panel.NodeInfo) {
	// fetch node info task
	c.nodeInfoMonitorPeriodic = &task.Task{
		Name:     "nodeInfoMonitor",
		Interval: node.PullInterval,
		Execute:  c.nodeInfoMonitor,
		ReloadCh: c.server.ReloadCh,
	}
	// fetch user list task
	c.userReportPeriodic = &task.Task{
		Name:     "reportUserTrafficTask",
		Interval: node.PushInterval,
		Execute:  c.reportUserTrafficTask,
		ReloadCh: c.server.ReloadCh,
	}
	log.WithField("tag", c.tag).Info("Start monitor node status")
	// delay to start nodeInfoMonitor
	_ = c.nodeInfoMonitorPeriodic.Start(false)
	log.WithField("tag", c.tag).Info("Start report node status")
	_ = c.userReportPeriodic.Start(false)
	c.userVersionPeriodic = &task.Task{
		Name:     "userVersionMonitor",
		Interval: userVersionInterval,
		Execute:  c.userVersionMonitor,
		ReloadCh: c.server.ReloadCh,
	}
	_ = c.userVersionPeriodic.Start(false)
	// behavior logging is opt-in per node (behavior.AddRecorder was only
	// called in Start() when the panel turned it on for this node), so only
	// pay for a periodic report cycle when there's actually something to
	// drain.
	if node.Common != nil && node.Common.BaseConfig != nil && node.Common.BaseConfig.BehaviorLogEnable {
		c.behaviorReportPeriodic = &task.Task{
			Name:     "reportBehaviorLogTask",
			Interval: node.PushInterval,
			Execute:  c.reportBehaviorLogTask,
			ReloadCh: c.server.ReloadCh,
		}
		log.WithField("tag", c.tag).Info("Start report behavior log")
		_ = c.behaviorReportPeriodic.Start(false)
	}
	if node.Security == panel.Tls {
		switch c.info.Common.CertInfo.CertMode {
		case "none", "", "file", "self", "remote":
		default:
			c.renewCertPeriodic = &task.Task{
				Name:     "renewCertTask",
				Interval: time.Hour * 24,
				Execute:  c.renewCertTask,
				ReloadCh: c.server.ReloadCh,
			}
			log.WithField("tag", c.tag).Info("Start renew cert")
			// delay to start renewCert
			_ = c.renewCertPeriodic.Start(true)
		}
	}
}

const (
	userVersionInterval = 3 * time.Second
	// userVersionBackoff is the number of intervals skipped after a failed
	// request, so a panel without the endpoint is asked about once a minute.
	userVersionBackoff = 20
)

// userVersionMonitor pulls the user list at once when the panel's user version
// changes, so speed limit changes don't wait for the pull interval.
func (c *Controller) userVersionMonitor(ctx context.Context) error {
	if c.userVersionSkip > 0 {
		c.userVersionSkip--
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, userVersionInterval)
	defer cancel()
	version, err := c.apiClient.GetUserVersion(cctx)
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Debug("Get user version failed")
		c.userVersionSkip = userVersionBackoff
		return nil
	}
	if c.userVersionSeen && version != c.userVersion {
		log.WithField("tag", c.tag).Info("User version changed, pull user list")
		c.nodeInfoMonitorPeriodic.Trigger()
	}
	c.userVersion = version
	c.userVersionSeen = true
	return nil
}

func (c *Controller) nodeInfoMonitor(ctx context.Context) (err error) {
	// retry a fault report that failed earlier
	c.syncFault()
	// get node info
	newN, err := c.apiClient.GetNodeInfo(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get node info failed")
		return nil
	}
	if newN != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
		}).Error("Got new node info, reload")
		if c.server.ReloadCh != nil {
			select {
			case c.server.ReloadCh <- struct{}{}:
			default:
			}
		} else {
			log.Panic("Reload failed")
		}
		// The reload rebuilds this node with fresh users on a new core, and
		// tears the current one down meanwhile, so don't touch it any more.
		return nil
	}
	log.WithField("tag", c.tag).Debug("Node info no change")

	// get user info
	newU, err := c.apiClient.GetUserList(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get user list failed")
		return nil
	}
	// get user alive
	newA, err := c.apiClient.GetUserAlive(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get alive list failed")
		return nil
	}

	// update alive list
	if newA != nil {
		c.limiter.AliveList = newA
	}
	// drop kicked devices
	c.applyKicks(c.apiClient.AliveMap.Kick)
	// node no changed, check users
	if len(newU) == 0 {
		log.WithField("tag", c.tag).Debug("User list no change")
		return nil
	}
	deleted, added, modified := compareUserList(c.userList, newU)
	// Open connections keep the bucket they started with, so they are dropped to
	// make clients reconnect when a user moves to another bucket (unlimited and
	// limited, own and shared limit, or another shared limit); changes of a
	// user's own limit are applied live by the bucket
	var speedToggled []panel.UserInfo
	var deviceChanged []panel.UserInfo
	if len(modified) > 0 {
		oldBucket := make(map[string]string, len(c.userList))
		oldDeviceLimit := make(map[string]int, len(c.userList))
		for _, u := range c.userList {
			oldBucket[u.Uuid] = limiter.BucketKey(c.tag, u)
			oldDeviceLimit[u.Uuid] = u.DeviceLimit
		}
		for _, u := range modified {
			if oldBucket[u.Uuid] != limiter.BucketKey(c.tag, u) {
				speedToggled = append(speedToggled, u)
			}
			if oldDeviceLimit[u.Uuid] != u.DeviceLimit {
				deviceChanged = append(deviceChanged, u)
			}
		}
	}
	if len(deleted) > 0 {
		// have deleted users
		err = c.server.DelUsers(deleted, c.tag, c.info)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Delete users failed")
			c.setFault("users", "delete users failed: "+err.Error())
			return nil
		}
	}
	if len(added) > 0 {
		// have added users
		_, err = c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			NodeInfo: c.info,
			Users:    added,
		})
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add users failed")
			c.setFault("users", "add users failed: "+err.Error())
			return nil
		}
	}
	if len(added) > 0 || len(deleted) > 0 || len(modified) > 0 {
		// update Limiter
		c.limiter.UpdateUser(c.tag, added, deleted, modified)
	}
	if len(deleted) > 0 {
		// Links opened after DelUsers closed the old ones, while the limiter still knew the user
		c.server.CloseUserLinks(c.tag, deleted)
	}
	if len(speedToggled) > 0 {
		c.server.CloseUserLinks(c.tag, speedToggled)
	}
	if len(deviceChanged) > 0 {
		go c.reclaimDevices(deviceChanged)
	}
	c.userList = newU
	c.userNum.Store(int64(len(newU)))
	c.updateSSUsers(deleted, added)
	c.clearFault("users")
	log.WithField("tag", c.tag).Infof("%d user deleted, %d user added, %d user modified", len(deleted), len(added), len(modified))
	return nil
}

// reclaimDevices claims again the connected devices of users whose device
// limit changed and drops those the panel now refuses: a device on one long
// connection opens nothing new that would be checked against a lowered limit.
func (c *Controller) reclaimDevices(users []panel.UserInfo) {
	open := c.server.OpenLinkIPs(c.tag)
	links := make(map[string][]string)
	for _, u := range users {
		email := format.UserTag(c.tag, u.Uuid)
		for _, ip := range open[email] {
			if c.limiter.Reclaim(context.Background(), email, ip) {
				links[u.Uuid] = append(links[u.Uuid], ip)
			}
		}
	}
	if len(links) > 0 {
		c.server.CloseIPLinks(c.tag, links)
	}
}

// applyKicks blocks the devices the panel kicked offline and drops their
// open connections on this node.
func (c *Controller) applyKicks(kicks map[int]map[string]int) {
	c.limiter.ApplyKicks(kicks)
	if len(kicks) == 0 {
		return
	}
	links := make(map[string][]string)
	for _, u := range c.userList {
		for ip := range kicks[u.Id] {
			links[u.Uuid] = append(links[u.Uuid], ip)
		}
	}
	if len(links) > 0 {
		c.server.CloseIPLinks(c.tag, links)
	}
}
