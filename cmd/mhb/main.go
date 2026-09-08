// mhb is the single binary for both sides of the bridge: `mhb broker` on the
// server, `mhb harness …` on developer machines.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/bambamboole/mattermost-harness-bridge/internal/cli"
)

func main() {
	if err := cli.NewRoot().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
