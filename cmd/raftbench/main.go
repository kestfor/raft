package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/term"
	"github.com/kestfor/raft/pkg/kvclient"
	"github.com/spf13/cobra"
)

type settings struct {
	Endpoint    string        `json:"endpoint"`
	Requests    int           `json:"requests"`
	Clients     int           `json:"clients"`
	Size        int           `json:"value_bytes"`
	Keyspace    int           `json:"keyspace"`
	Tests       []string      `json:"tests"`
	ReadPercent int           `json:"mixed_read_percent"`
	Warmup      int           `json:"warmup_requests_per_workload"`
	Repeat      int           `json:"repeats"`
	Timeout     time.Duration `json:"timeout_ns"`
}

type report struct {
	Started   time.Time `json:"started_at"`
	GoVersion string    `json:"go_version"`
	Platform  string    `json:"platform"`
	CPUs      int       `json:"logical_cpus"`
	Settings  settings  `json:"settings"`
	Prefix    string    `json:"key_prefix"`
	Results   []result  `json:"results"`
	Error     string    `json:"error,omitempty"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := newCommand().ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func newCommand() *cobra.Command {
	var cfg settings
	var asJSON bool
	var colorMode string

	cmd := &cobra.Command{
		Use:           "raftbench",
		Short:         "Measure a Raft KV cluster: throughput, latency and errors",
		Long:          "Run fixed-concurrency HTTP workloads against a Raft KV node.\nRequests follow leader redirects; each worker reuses HTTP connections.\nGET verifies values and includes the server's fresh quorum confirmation.\nSetup and warmup are excluded. Only unique raftbench/* keys are written.",
		Example:       "  raftbench -n 1000 -c 8 -d 256\n  raftbench --endpoint http://localhost:18003 -t get,put,mixed --repeat 3\n  raftbench -t mixed --read-percent 80 -c 32 --json > results.json\n  raftbench --endpoint http://localhost:18003 -t put -n 100000 -c 512 --warmup 100",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if colorMode != "auto" && colorMode != "always" && colorMode != "never" {
				return fmt.Errorf("--color must be auto, always or never")
			}

			if err := cfg.validate(); err != nil {
				return err
			}

			return run(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), cfg, asJSON, colorMode)
		},
	}

	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	f := cmd.Flags()
	f.StringVar(
		&cfg.Endpoint,
		"endpoint",
		"http://localhost:18001",
		"REST origin; use the leader to exclude redirect overhead",
	)
	f.IntVarP(&cfg.Requests, "requests", "n", 1000, "measured requests per workload and repeat (1..1000000)")
	f.IntVarP(&cfg.Clients, "clients", "c", 8, "parallel HTTP clients (1..1024), one request in flight per client")
	f.IntVarP(&cfg.Size, "size", "d", 256, "value size in bytes (0..524288)")
	f.IntVarP(&cfg.Keyspace, "keyspace", "r", 64, "number of private benchmark keys (1..10000)")
	f.StringSliceVarP(&cfg.Tests, "tests", "t", []string{"put", "get", "mixed"}, "workloads: put,get,mixed")
	f.IntVar(&cfg.ReadPercent, "read-percent", 80, "GET percentage in mixed workload")
	f.IntVar(&cfg.Warmup, "warmup", 50, "unmeasured requests before each workload")
	f.IntVar(&cfg.Repeat, "repeat", 1, "repeat the workload suite (1..100)")
	f.DurationVar(&cfg.Timeout, "timeout", 5*time.Second, "per-request timeout including redirects")
	f.BoolVar(&asJSON, "json", false, "write structured results to stdout, without terminal formatting")
	f.StringVar(&colorMode, "color", "auto", "color output: auto, always, never (NO_COLOR disables)")

	return cmd
}

func (s settings) validate() error {
	if s.Requests < 1 || s.Requests > 1_000_000 || s.Clients < 1 || s.Clients > 1024 || s.Clients > s.Requests {
		return fmt.Errorf("require 1 <= clients <= min(requests, 1024) and 1 <= requests <= 1000000")
	}

	if s.Size < 0 || s.Size > 512<<10 || s.Keyspace < 1 || s.Keyspace > 10000 {
		return fmt.Errorf("size must be 0..524288 bytes and keyspace 1..10000")
	}

	if s.ReadPercent < 0 || s.ReadPercent > 100 || s.Warmup < 0 || s.Warmup > 1_000_000 || s.Repeat < 1 ||
		s.Repeat > 100 {
		return fmt.Errorf("invalid read-percent (0..100), warmup (0..1000000), or repeat (1..100)")
	}

	if len(s.Tests) == 0 {
		return fmt.Errorf("select at least one workload")
	}

	for _, test := range s.Tests {
		if test != "put" && test != "get" && test != "mixed" {
			return fmt.Errorf("unknown workload %q", test)
		}
	}

	return nil
}

func run(ctx context.Context, out, errOut io.Writer, cfg settings, asJSON bool, colorMode string) (runErr error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = cfg.Clients * 2
	transport.MaxIdleConnsPerHost = cfg.Clients
	transport.MaxConnsPerHost = cfg.Clients
	defer transport.CloseIdleConnections()

	client, err := kvclient.NewWithTransport(cfg.Endpoint, cfg.Timeout, transport)
	if err != nil {
		return err
	}

	state, err := client.Status(ctx)
	if err != nil {
		return fmt.Errorf("initial status: %w", err)
	}

	r := report{
		Started:   time.Now().UTC(),
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
		CPUs:      runtime.NumCPU(),
		Settings:  cfg,
		Prefix:    fmt.Sprintf("raftbench/%d/", time.Now().UnixNano()),
	}

	// Preserve earlier measurements even if a later warmup or status check fails.
	if asJSON {
		defer func() {
			if runErr != nil {
				r.Error = runErr.Error()
			}

			encoder := json.NewEncoder(out)
			encoder.SetIndent("", "  ")
			runErr = errors.Join(runErr, encoder.Encode(r))
		}()
	}

	if !asJSON {
		_, _ = fmt.Fprintln(
			out,
			"\n"+tone(
				out,
				colorMode,
				"RAFTBENCH",
				"6",
				true,
			)+"  "+tone(
				out,
				colorMode,
				"CLUSTER PERFORMANCE",
				"8",
				false,
			),
		)
		_, _ = fmt.Fprintln(out, tone(out, colorMode, strings.Repeat("─", 70), "8", false))
		_, _ = fmt.Fprintf(
			out,
			"Endpoint  %s (%s, term %d, leader %s)\nLoad      %d requests · %d clients · %d B · %d keys · %d repeat(s)\n",
			cfg.Endpoint,
			state.Role,
			state.Term,
			state.LeaderID,
			cfg.Requests,
			cfg.Clients,
			cfg.Size,
			cfg.Keyspace,
			cfg.Repeat,
		)
		_, _ = fmt.Fprintln(out, "Mode      closed-loop · success latencies · setup/warmup excluded")
		_, _ = fmt.Fprintln(out, "Keys      "+r.Prefix+"* (retained after the run)")
		if state.Role != "leader" {
			_, _ = fmt.Fprintln(
				out,
				tone(out, colorMode, "Note      follower endpoint: redirect overhead is included", "3", false),
			)
		}

		_, _ = fmt.Fprintln(errOut, tone(errOut, colorMode, "Preparing keyspace…", "8", false))
	}

	value := bytes.Repeat([]byte("x"), cfg.Size)
	for i := range cfg.Keyspace {
		if _, err := client.Put(ctx, r.Prefix+strconv.Itoa(i), value); err != nil {
			return fmt.Errorf("prepare key %d: %w", i, err)
		}
	}

	failed := false
	for repeat := range cfg.Repeat {
		for _, workload := range cfg.Tests {
			if cfg.Warmup > 0 {
				warm := cfg
				warm.Requests = cfg.Warmup
				wr := benchmark(ctx, client, warm, workload, r.Prefix, &progress{})
				if wr.Errors > 0 || ctx.Err() != nil {
					return fmt.Errorf("warmup failed: %v (context: %v)", wr.ErrorCounts, ctx.Err())
				}
			}

			before, err := client.Status(ctx)
			if err != nil {
				return fmt.Errorf("status before workload: %w", err)
			}

			p := &progress{}
			stop := showProgress(errOut, workload, cfg.Requests, p, !asJSON, colorMode)
			item := benchmark(ctx, client, cfg, workload, r.Prefix, p)
			stop()

			item.Repeat = repeat + 1
			item.Before = before

			after, statusErr := client.Status(ctx)
			item.After = after
			if statusErr != nil {
				item.StatusError = statusErr.Error()
				failed = true
			}

			r.Results = append(r.Results, item)
			failed = failed || item.Errors > 0 || item.Completed != item.Requested
			if !asJSON {
				printResult(out, item, colorMode)
			}

			if ctx.Err() != nil {
				break
			}
		}

		if ctx.Err() != nil {
			break
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if failed {
		return fmt.Errorf("benchmark completed with errors; see results")
	}

	return nil
}

func printResult(out io.Writer, r result, colorMode string) {
	color := "6"
	if r.Workload == "put" {
		color = "5"
	}
	if r.Workload == "get" {
		color = "4"
	}

	_, _ = fmt.Fprintf(out, "\n%s  %s\n",
		tone(out, colorMode, strings.ToUpper(r.Workload), color, true),
		tone(out, colorMode, fmt.Sprintf("repeat %d · %.2fs", r.Repeat, r.Seconds), "8", false))

	errorColor := "2"
	if r.Errors > 0 {
		errorColor = "1"
	}
	_, _ = fmt.Fprintf(out, "  %s    %s    %s\n",
		tone(out, colorMode, fmt.Sprintf("%.1f req/s", r.Throughput), "2", true),
		fmt.Sprintf("%d/%d successful", r.Success, r.Requested),
		tone(out, colorMode, fmt.Sprintf("%d errors", r.Errors), errorColor, r.Errors > 0))

	_, _ = fmt.Fprintln(out, tone(out, colorMode,
		fmt.Sprintf("  %-10s %10s %10s %10s %10s", "Latency", "p50", "p95", "p99", "max"), "8", false))
	if l := r.Latency; l != nil {
		_, _ = fmt.Fprintf(out, "  %-10s %s %s %s %s\n", "ms",
			tone(out, colorMode, fmt.Sprintf("%10.2f", l.P50), "6", false),
			tone(out, colorMode, fmt.Sprintf("%10.2f", l.P95), "6", false),
			tone(out, colorMode, fmt.Sprintf("%10.2f", l.P99), "3", false),
			tone(out, colorMode, fmt.Sprintf("%10.2f", l.Max), "3", false))
	} else {
		_, _ = fmt.Fprintf(out, "  %-10s %10s %10s %10s %10s\n", "ms", "—", "—", "—", "—")
	}
	_, _ = fmt.Fprintln(out, tone(out, colorMode,
		fmt.Sprintf("  Payload %.2f MiB/s · success latencies only", r.PayloadMiBPerSecond), "8", false))

	if r.Workload == "mixed" {
		for _, name := range []string{"get", "put"} {
			op := r.Operations[name]
			if op.Latency != nil {
				_, _ = fmt.Fprintf(out, "  %-3s  %d ok / %d errors · p50 %.2f ms · p99 %.2f ms\n",
					strings.ToUpper(name), op.Success, op.Errors, op.Latency.P50, op.Latency.P99)
			}
		}
	}

	if len(r.ErrorCounts) > 0 {
		_, _ = fmt.Fprintln(out, tone(out, colorMode, fmt.Sprintf("  Errors: %v", r.ErrorCounts), "1", true))
	}
	if r.StatusError != "" {
		_, _ = fmt.Fprintln(out, tone(out, colorMode, "  Status check failed: "+r.StatusError, "1", false))
	}
	if r.Before.Term != r.After.Term && r.StatusError == "" {
		_, _ = fmt.Fprintln(out, tone(out, colorMode,
			fmt.Sprintf("  Leadership term changed: %d → %d", r.Before.Term, r.After.Term), "3", true))
	}
}

func terminal(w io.Writer) bool {
	file, ok := w.(*os.File)
	return ok && term.IsTerminal(file.Fd())
}

func tone(w io.Writer, mode, text, color string, bold bool) string {
	if mode == "never" || os.Getenv("NO_COLOR") != "" ||
		(mode != "always" && (!terminal(w) || os.Getenv("TERM") == "dumb")) {
		return text
	}
	return lipgloss.NewStyle().Bold(bold).Foreground(lipgloss.Color(color)).Render(text)
}

func showProgress(w io.Writer, name string, total int, p *progress, enabled bool, colorMode string) func() {
	if !enabled || !terminal(w) {
		return func() {}
	}

	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		start := time.Now()

		for {
			select {
			case <-done:
				_, _ = fmt.Fprint(w, "\r\x1b[2K")
				return
			case <-ticker.C:
				count := int(p.completed.Load())
				bars := min(24, count*24/total)
				bar := tone(w, colorMode, strings.Repeat("━", bars), "6", true) +
					tone(w, colorMode, strings.Repeat("─", 24-bars), "8", false)
				errorColor := "2"
				if p.failures.Load() > 0 {
					errorColor = "1"
				}
				_, _ = fmt.Fprintf(w, "\r\x1b[2K%s  %s %3d%%  %d/%d · %.0f req/s · %s",
					tone(w, colorMode, fmt.Sprintf("%-5s", strings.ToUpper(name)), "6", true),
					bar, count*100/total, count, total, float64(count)/time.Since(start).Seconds(),
					tone(w, colorMode, fmt.Sprintf("%d errors", p.failures.Load()), errorColor, false))
			}
		}
	}()

	return func() { close(done); <-stopped }
}
