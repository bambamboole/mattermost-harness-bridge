// harness runs on a developer's machine and executes jobs the broker hands it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/harness"
	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/permission"
	"github.com/bambamboole/mattermost-harness-bridge/internal/version"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(os.Args[2:])
	case "pair":
		err = pairCmd(os.Args[2:])
	case "workspace":
		err = workspaceCmd(os.Args[2:])
	case "mcp-permissions":
		err = mcpCmd(os.Args[2:])
	case "config":
		err = configCmd()
	case "version":
		fmt.Println(version.Version)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  harness pair --broker https://broker.example.com <code>   pair this machine (code from the bot)
  harness workspace add <name> <dir>                         allow a directory
  harness workspace rm <name>
  harness run                                                 start the daemon
  harness config                                              print the config path and content
  harness mcp-permissions --socket <path> --job <id>          (spawned by claude, not for humans)`)
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	debug := fs.Bool("debug", false, "verbose logging")
	_ = fs.Parse(args)
	lvl := slog.LevelInfo
	if *debug {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	cfg, err := harness.LoadConfig()
	if err != nil {
		return err
	}
	h, err := harness.New(cfg, log)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	err = h.Run(ctx)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func pairCmd(args []string) error {
	fs := flag.NewFlagSet("pair", flag.ExitOnError)
	broker := fs.String("broker", "", "broker base URL, e.g. https://broker.example.com")
	name := fs.String("name", "", "name for this harness (default: hostname)")
	_ = fs.Parse(args)
	if *broker == "" || fs.NArg() != 1 {
		return fmt.Errorf("usage: harness pair --broker <url> <code>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := harness.Pair(ctx, *broker, fs.Arg(0), *name)
	if err != nil {
		return err
	}
	fmt.Printf("paired as %s (harness %s), config written to %s\n", cfg.OwnerMMUserID, cfg.HarnessID, harness.ConfigPath())
	if len(cfg.Workspaces) == 0 {
		fmt.Println("next: harness workspace add <name> <dir>, then harness run")
	}
	return nil
}

func workspaceCmd(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: harness workspace add <name> <dir> | rm <name>")
	}
	cfg, err := harness.LoadConfig()
	if err != nil {
		return err
	}
	switch args[0] {
	case "add":
		if len(args) != 3 {
			return fmt.Errorf("usage: harness workspace add <name> <dir>")
		}
		dir, err := absDir(args[2])
		if err != nil {
			return err
		}
		cfg.Workspaces[args[1]] = dir
	case "rm":
		delete(cfg.Workspaces, args[1])
	default:
		return fmt.Errorf("unknown workspace subcommand %q", args[0])
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	return harness.SaveConfig(cfg)
}

func configCmd() error {
	fmt.Println(harness.ConfigPath())
	b, err := os.ReadFile(harness.ConfigPath())
	if err != nil {
		return err
	}
	_, _ = os.Stdout.Write(b)
	return nil
}

func mcpCmd(args []string) error {
	fs := flag.NewFlagSet("mcp-permissions", flag.ExitOnError)
	socket := fs.String("socket", "", "daemon permission socket")
	job := fs.String("job", "", "job id")
	timeout := fs.Duration("timeout", 2*time.Hour, "max wait per approval")
	_ = fs.Parse(args)
	if *socket == "" || *job == "" {
		return fmt.Errorf("--socket and --job are required")
	}
	return permission.ServeStdio(context.Background(), *socket, *job, *timeout)
}
