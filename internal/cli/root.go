// Package cli defines the mhb command tree. One binary serves both roles:
// `mhb broker` on the server, `mhb harness …` on developer machines.
package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/bambamboole/mattermost-harness-bridge/internal/version"
)

func NewRoot() *cobra.Command {
	root := &cobra.Command{
		Use:           "mhb",
		Short:         "Mattermost ↔ Claude Code harness bridge",
		Long:          "mhb runs the broker next to Mattermost (`mhb broker`) and the harness on each developer machine (`mhb harness run`).",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version.Version,
	}
	root.AddCommand(newBrokerCmd(), newHarnessCmd(), &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run:   func(cmd *cobra.Command, args []string) { _, _ = fmt.Fprintln(cmd.OutOrStdout(), version.Version) },
	})
	return root
}
