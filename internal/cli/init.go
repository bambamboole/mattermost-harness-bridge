package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/bambamboole/mattermost-harness-bridge/internal/harness"
)

func newHarnessInitCmd() *cobra.Command {
	var broker, botName, name, agentName, defaultWorkspace string
	var workspaces []string
	var yes bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Onboard this machine: pair with the broker and create your bot",
		Long: `Asks the broker for a code, waits until you type "/harness init <code>" in
any Mattermost channel, and writes the config. The first init creates your
own bot (@harness-<username> unless --bot says otherwise); later inits add
more machines to it. Then it asks for the agent and the workspaces.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			out := cmd.OutOrStdout()
			in := bufio.NewReader(cmd.InOrStdin())
			if broker == "" {
				broker = ask(in, out, "Broker URL (https://…)", "")
				if broker == "" {
					return errors.New("a broker URL is required")
				}
			}
			host, _ := os.Hostname()
			if name == "" {
				name = host
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Minute)
			defer cancel()

			start, err := harness.StartInit(ctx, broker, botName, name)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(out, "\nIn Mattermost, in a channel your bot should join, type:\n\n    %s\n\nWaiting (code valid until %s) ", start.Command, start.ExpiresAt.Local().Format(time.Kitchen))
			res, err := harness.WaitInit(ctx, broker, start.Code, start.ExpiresAt, func() { _, _ = fmt.Fprint(out, ".") })
			_, _ = fmt.Fprintln(out)
			if err != nil {
				return err
			}
			cfg, err := harness.ApplyInit(broker, res, name)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(out, "Paired as @%s with bot @%s.\n", res.Username, res.BotUsername)

			// Agent: whichever binaries are present; the default is claude if it exists.
			available := detectAgents(cfg)
			if len(available) == 0 {
				_, _ = fmt.Fprintln(out, "Neither claude nor codex was found on PATH; install one before running the harness.")
			}
			if agentName == "" && !yes && len(available) > 1 {
				agentName = ask(in, out, fmt.Sprintf("Default agent (%s)", strings.Join(available, "/")), cfg.Agent)
			} else if agentName == "" && len(available) == 1 {
				agentName = available[0]
			}
			if agentName != "" {
				cfg.Agent = agentName
			}
			if defaultWorkspace != "" {
				if cfg.DefaultWorkspace, err = filepath.Abs(defaultWorkspace); err != nil {
					return err
				}
			}
			for _, ws := range workspaces {
				n, dir, ok := strings.Cut(ws, "=")
				if !ok {
					return fmt.Errorf("--workspace %q: expected name=dir", ws)
				}
				abs, err := absDir(dir)
				if err != nil {
					return err
				}
				cfg.Workspaces[n] = abs
			}
			if !yes {
				for {
					entry := ask(in, out, "Add a workspace as name=dir (empty to finish)", "")
					if entry == "" {
						break
					}
					n, dir, ok := strings.Cut(entry, "=")
					if !ok {
						_, _ = fmt.Fprintln(out, "expected name=dir")
						continue
					}
					abs, err := absDir(strings.TrimSpace(dir))
					if err != nil {
						_, _ = fmt.Fprintln(out, err)
						continue
					}
					cfg.Workspaces[strings.TrimSpace(n)] = abs
				}
			}
			if err := cfg.Validate(); err != nil {
				return err
			}
			if err := harness.SaveConfig(cfg); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(out, "\nConfig written to %s\n", harness.ConfigPath())
			_, _ = fmt.Fprintf(out, "Default workspace: %s (jobs without ws: run here)\n", cfg.DefaultWorkspace)
			_, _ = fmt.Fprintf(out, "Agent: %s\n", cfg.Agent)
			names := cfg.WorkspaceNames()
			sort.Strings(names)
			if len(names) > 0 {
				_, _ = fmt.Fprintf(out, "Workspaces: %s\n", strings.Join(names, ", "))
			}
			_, _ = fmt.Fprintf(out, "\nStart it with: mhb harness run\nThen mention @%s in a channel it is in; /harness join brings it into more.\n", res.BotUsername)
			return nil
		},
	}
	cmd.Flags().StringVar(&broker, "broker", "", "broker base URL, e.g. https://broker.example.com (asked if missing)")
	cmd.Flags().StringVar(&botName, "bot", "", "username for your bot (default: harness-<your username>; only used by the first init)")
	cmd.Flags().StringVar(&name, "name", "", "name for this machine (default: hostname)")
	cmd.Flags().StringVar(&agentName, "agent", "", "default agent: claude or codex (asked if both are installed)")
	cmd.Flags().StringVar(&defaultWorkspace, "default-workspace", "", "directory for jobs without ws: (default ~/.harness)")
	cmd.Flags().StringArrayVar(&workspaces, "workspace", nil, "workspace as name=dir (repeatable)")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "no prompts: take flags and defaults")
	return cmd
}

func detectAgents(cfg harness.Config) []string {
	var out []string
	if _, err := exec.LookPath(cfg.ClaudeBin); err == nil {
		out = append(out, "claude")
	}
	if _, err := exec.LookPath(cfg.CodexBin); err == nil {
		out = append(out, "codex")
	}
	return out
}

func ask(in *bufio.Reader, out io.Writer, prompt, def string) string {
	if def != "" {
		_, _ = fmt.Fprintf(out, "%s [%s]: ", prompt, def)
	} else {
		_, _ = fmt.Fprintf(out, "%s: ", prompt)
	}
	line, err := in.ReadString('\n')
	if err != nil && line == "" {
		return def
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return def
	}
	return line
}
