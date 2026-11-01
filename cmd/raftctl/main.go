package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"text/tabwriter"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/kestfor/raft/pkg/kvclient"
	"github.com/spf13/cobra"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := newCommand().ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newCommand() *cobra.Command {
	var endpoint string
	var timeout time.Duration
	var jsonOutput bool
	var client *kvclient.Client
	root := &cobra.Command{
		Use:           "raftctl",
		Short:         "A small, replicated key/value store",
		Long:          "Read and write a Raft-backed store. Connect to any node; KV requests follow leader redirects.\nValues are bytes. GET writes exact bytes to stdout, suitable for pipes and files.",
		Example:       "  raftctl put greeting 'Hello, Raft!'\n  raftctl get greeting\n  raftctl --endpoint http://localhost:18002 status\n  cat image.png | raftctl put image --stdin",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			var err error
			client, err = kvclient.New(endpoint, timeout)
			return err
		},
	}

	root.SetIn(os.Stdin)
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)
	root.PersistentFlags().StringVar(&endpoint, "endpoint", "http://localhost:18001", "REST origin of any node")
	root.PersistentFlags().DurationVar(&timeout, "timeout", 5*time.Second, "total request timeout, including redirects")
	root.PersistentFlags().BoolVar(&jsonOutput, "json", false, "machine-readable output (values encoded as base64)")

	var fromFile string
	var fromStdin bool
	put := &cobra.Command{
		Use:     "put KEY [VALUE]",
		Short:   "Create or replace a value",
		Example: "  raftctl put name Alice\n  raftctl put document --file ./document.bin\n  printf hello | raftctl put greeting --stdin",
		Args:    cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 2 && (fromStdin || cmd.Flags().Changed("file"))) ||
				(len(args) == 1 && !fromStdin && !cmd.Flags().Changed("file")) {
				return fmt.Errorf("provide exactly one of VALUE, --stdin or --file")
			}

			var input io.Reader
			switch {
			case fromStdin:
				input = cmd.InOrStdin()
			case cmd.Flags().Changed("file"):
				file, err := os.Open(fromFile)
				if err != nil {
					return err
				}

				defer func() { _ = file.Close() }()
				input = file
			default:
				input = strings.NewReader(args[1])
			}

			value, err := io.ReadAll(io.LimitReader(input, (512<<10)+1))
			if err != nil {
				return err
			}

			if len(value) > 512<<10 {
				return fmt.Errorf("value exceeds 512 KiB")
			}

			index, err := client.Put(cmd.Context(), args[0], value)
			if err != nil {
				return err
			}

			return writeMutation(cmd, jsonOutput, "stored", args[0], index)
		},
	}

	put.Flags().StringVar(&fromFile, "file", "", "read value from a file")
	put.Flags().BoolVar(&fromStdin, "stdin", false, "read value from stdin")
	put.MarkFlagsMutuallyExclusive("file", "stdin")

	var output string
	get := &cobra.Command{
		Use: "get KEY", Short: "Read a value with a fresh quorum confirmation", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if jsonOutput && cmd.Flags().Changed("output") {
				return fmt.Errorf("--json and --output cannot be combined")
			}

			value, err := client.Get(cmd.Context(), args[0])
			if err != nil {
				return err
			}

			if jsonOutput {
				// []byte has an unambiguous base64 representation, including binary values.
				if value == nil {
					value = []byte{}
				}

				return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
					Key   string `json:"key"`
					Value []byte `json:"value"`
				}{args[0], value})
			}

			if cmd.Flags().Changed("output") {
				return os.WriteFile(output, value, 0o600)
			}

			_, err = cmd.OutOrStdout().Write(value)
			return err
		},
	}

	get.Flags().StringVar(&output, "output", "", "write exact bytes to this file")

	del := &cobra.Command{
		Use:     "delete KEY",
		Aliases: []string{"del"},
		Short:   "Delete a key (missing keys are accepted)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			index, err := client.Delete(cmd.Context(), args[0])
			if err != nil {
				return err
			}

			return writeMutation(cmd, jsonOutput, "deleted", args[0], index)
		},
	}

	status := &cobra.Command{
		Use: "status", Short: "Show the contacted node's role and replication progress", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			state, err := client.Status(cmd.Context())
			if err != nil {
				return err
			}

			if jsonOutput {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(state)
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 3, ' ', 0)
			_, _ = fmt.Fprintln(w, heading(cmd, "RAFT · NODE STATUS"))
			_, _ = fmt.Fprintf(
				w,
				"Node\t%q\nRole\t%s\nTerm\t%d\nLeader\t%q\nAvailable\t%t\nLog\t%d\nCommitted\t%d\nApplied\t%d\n",
				state.ID,
				state.Role,
				state.Term,
				state.LeaderID,
				state.Available,
				state.LastLogIndex,
				state.CommitIndex,
				state.AppliedIndex,
			)
			return w.Flush()
		},
	}

	root.AddCommand(put, get, del, status)
	return root
}

func writeMutation(cmd *cobra.Command, asJSON bool, action, key string, index uint64) error {
	if asJSON {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
			Action string `json:"action"`
			Key    string `json:"key"`
			Index  uint64 `json:"index"`
		}{action, key, index})
	}

	_, err := fmt.Fprintf(cmd.OutOrStdout(), "%s %q · index %d\n", heading(cmd, action), key, index)
	return err
}

func heading(cmd *cobra.Command, text string) string {
	if file, ok := cmd.OutOrStdout().(*os.File); ok && os.Getenv("NO_COLOR") == "" {
		if info, err := file.Stat(); err == nil && info.Mode()&os.ModeCharDevice != 0 {
			return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6")).Render(text)
		}
	}

	return text
}
