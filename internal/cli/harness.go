package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/bambamboole/mattermost-harness-bridge/internal/harness"
	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/permission"
)

func newHarnessCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "harness",
		Short: "Run and manage the harness on this machine",
	}
	cmd.AddCommand(newHarnessInitCmd(), newHarnessRunCmd(), newHarnessPairCmd(), newHarnessWorkspaceCmd(), newHarnessConfigCmd(), newHarnessMCPCmd())
	return cmd
}

func newHarnessRunCmd() *cobra.Command {
	var debug bool
	var defaultWorkspace, agentName string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Start the harness daemon",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			lvl := slog.LevelInfo
			if debug {
				lvl = slog.LevelDebug
			}
			log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
			cfg, err := harness.LoadConfig()
			if err != nil {
				return err
			}
			if defaultWorkspace != "" {
				if cfg.DefaultWorkspace, err = filepath.Abs(defaultWorkspace); err != nil {
					return err
				}
			}
			if agentName != "" {
				cfg.Agent = agentName
			}
			if err := cfg.Validate(); err != nil {
				return err
			}
			h, err := harness.New(cfg, log)
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			err = h.Run(ctx)
			if ctx.Err() != nil {
				return nil
			}
			return err
		},
	}
	cmd.Flags().BoolVar(&debug, "debug", false, "verbose logging")
	cmd.Flags().StringVar(&defaultWorkspace, "default-workspace", "", "directory for jobs that name no workspace (default: default_workspace in the config, ~/.harness)")
	cmd.Flags().StringVar(&agentName, "agent", "", "coding agent for jobs that name none: claude or codex (default: agent in the config)")
	return cmd
}

func newHarnessPairCmd() *cobra.Command {
	var broker, name string
	cmd := &cobra.Command{
		Use:   "pair <code>",
		Short: "Pair with a code from a direct message to the shared bot (brokers without onboarding)",
		Long:  "Send `pair` to the shared bot in a Mattermost direct message; it replies with a one-time code. Prefer `mhb harness init`.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			cfg, err := harness.Pair(ctx, broker, args[0], name)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "paired as %s (harness %s), config written to %s\n", cfg.OwnerMMUserID, cfg.HarnessID, harness.ConfigPath())
			if len(cfg.Workspaces) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "next: mhb harness workspace add <name> <dir>, then mhb harness run")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&broker, "broker", "", "broker base URL, e.g. https://broker.example.com")
	cmd.Flags().StringVar(&name, "name", "", "name for this harness (default: hostname)")
	_ = cmd.MarkFlagRequired("broker")
	return cmd
}

func newHarnessWorkspaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workspace",
		Short: "Manage the directories jobs may run in",
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "add <name> <dir>",
			Short: "Allow a directory under a short name",
			Args:  cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				cfg, err := harness.LoadConfig()
				if err != nil {
					return err
				}
				dir, err := absDir(args[1])
				if err != nil {
					return err
				}
				cfg.Workspaces[args[0]] = dir
				if err := cfg.Validate(); err != nil {
					return err
				}
				return harness.SaveConfig(cfg)
			},
		},
		&cobra.Command{
			Use:   "rm <name>",
			Short: "Remove a workspace",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				cfg, err := harness.LoadConfig()
				if err != nil {
					return err
				}
				if _, ok := cfg.Workspaces[args[0]]; !ok {
					return fmt.Errorf("no workspace %q", args[0])
				}
				delete(cfg.Workspaces, args[0])
				return harness.SaveConfig(cfg)
			},
		},
		&cobra.Command{
			Use:   "list",
			Short: "List workspaces",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				cfg, err := harness.LoadConfig()
				if err != nil {
					return err
				}
				names := cfg.WorkspaceNames()
				sort.Strings(names)
				for _, n := range names {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", n, cfg.Workspaces[n])
				}
				return nil
			},
		},
	)
	return cmd
}

func newHarnessConfigCmd() *cobra.Command {
	var showSecrets bool
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Print the config path and content",
		Long:  "Prints the config. The harness token is redacted unless --show-secrets is given.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), harness.ConfigPath())
			b, err := os.ReadFile(harness.ConfigPath())
			if err != nil {
				return err
			}
			if !showSecrets {
				if b, err = harness.RedactConfig(b); err != nil {
					return err
				}
			}
			_, _ = cmd.OutOrStdout().Write(b)
			return nil
		},
	}
	cmd.Flags().BoolVar(&showSecrets, "show-secrets", false, "print the harness token instead of redacting it")
	return cmd
}

func newHarnessMCPCmd() *cobra.Command {
	var socket, job string
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:    "mcp-permissions",
		Short:  "MCP permission server spawned by claude; not for interactive use",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return permission.ServeStdio(cmd.Context(), socket, job, timeout)
		},
	}
	cmd.Flags().StringVar(&socket, "socket", "", "daemon permission socket")
	cmd.Flags().StringVar(&job, "job", "", "job id")
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Hour, "max wait per approval")
	_ = cmd.MarkFlagRequired("socket")
	_ = cmd.MarkFlagRequired("job")
	return cmd
}

func absDir(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return "", err
	}
	if !st.IsDir() {
		return "", fmt.Errorf("%s is not a directory", abs)
	}
	return abs, nil
}
