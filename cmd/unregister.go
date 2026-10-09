package cmd

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	panel "github.com/wyusgw/v2node/api/v2board"
	"github.com/wyusgw/v2node/conf"
)

var unregisterConfig string

var unregisterCommand = cobra.Command{
	Use:   "unregister",
	Short: "Tell the panel the nodes in a config file are uninstalled",
	Run:   unregisterHandle,
	Args:  cobra.NoArgs,
}

func init() {
	unregisterCommand.PersistentFlags().
		StringVarP(&unregisterConfig, "config", "c",
			"/etc/v2node/config.json", "config file path")
	command.AddCommand(&unregisterCommand)
}

func unregisterHandle(_ *cobra.Command, _ []string) {
	c := conf.New()
	if err := c.LoadFromPath(unregisterConfig); err != nil {
		log.WithField("err", err).Error("Load config file failed")
		return
	}
	for i := range c.NodeConfigs {
		node := c.NodeConfigs[i]
		p, err := panel.New(&node)
		if err != nil {
			log.WithField("err", err).Error("Create panel client failed")
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = p.ReportUninstall(ctx)
		cancel()
		if err != nil {
			log.WithFields(log.Fields{
				"node": node.NodeID,
				"err":  err,
			}).Error("Report uninstall failed")
			continue
		}
		log.WithField("node", node.NodeID).Info("Reported uninstall to the panel")
	}
}
