package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

var (
	backupName      string
	collectionNames string
	force           bool
)

var createBackupCmd = &cobra.Command{
	Use:   "create",
	Short: "create subcommand create a backup.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		fmt.Println("config:" + config)
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		var collectionNameArr []string
		if collectionNames == "" {
			collectionNameArr = []string{}
		} else {
			collectionNameArr = strings.Split(collectionNames, ",")
		}
		resp := backupContext.CreateBackup(context, &backuppb.CreateBackupRequest{
			BackupName:      backupName,
			CollectionNames: collectionNameArr,
			Force:           force,
		})
		fmt.Println(resp.GetCode(), "\n", resp.GetMsg())
	},
}

func init() {
	createBackupCmd.Flags().StringVarP(&backupName, "name", "n", "", "backup name, if unset will generate a name automatically")
	createBackupCmd.Flags().StringVarP(&collectionNames, "colls", "", "", "collectionNames to backup, use ',' to connect multiple collections")
	createBackupCmd.Flags().BoolVarP(&force, "force", "f", false, "force backup skip flush, should make sure data has been stored into disk when using it")

	rootCmd.AddCommand(createBackupCmd)
}
