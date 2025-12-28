package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var levels = []string{"info", "debug", "warn", "error", "trace", "fatal"}

var words = []string{
	"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
	"india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
	"quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray",
	"yankee", "zulu", "orange", "banana", "coffee", "memory", "network", "packet",
	"kernel", "thread", "buffer", "system", "signal", "random", "future", "garden",
	"window", "mountain", "river", "forest", "ocean", "station", "library", "planet",
	"engine", "sensor", "rocket", "mirror", "stream", "cursor", "script", "folder",
	"cloud", "object", "bucket", "prefix", "profile", "transfer", "sync", "retry",
}

func makeMessage(r *rand.Rand, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	var b strings.Builder
	for {
		w := words[r.Intn(len(words))]
		// add space if not first word
		need := len(w)
		if b.Len() > 0 {
			need += 1
		}
		if b.Len()+need > maxLen {
			break
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(w)
	}
	if b.Len() == 0 {
		// fall back to a truncated single word
		w := words[r.Intn(len(words))]
		if len(w) > maxLen {
			w = w[:maxLen]
		}
		b.WriteString(w)
	}
	return b.String()
}

func runSpammer(ctx context.Context, w io.Writer, rate float64, lineLen int, r *rand.Rand) {
	if rate <= 0 {
		<-ctx.Done()
		return
	}

	period := time.Duration(float64(time.Second) / rate)
	if period <= 0 {
		period = time.Nanosecond
	}

	next := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		now := time.Now()
		if now.Before(next) {
			timer := time.NewTimer(next.Sub(now))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		} else if now.Sub(next) > 10*period {
			// avoid huge catch-up bursts after long pauses
			next = now
		}

		ts := time.Now().Format("2006-01-02 15:04:05.000")
		level := levels[r.Intn(len(levels))]
		msg := makeMessage(r, lineLen)
		_, _ = fmt.Fprintf(w, "%s [%s] %s\n", ts, level, msg)

		next = next.Add(period)
	}
}

func openOutFile(path string, overwrite bool) (*os.File, error) {
	flags := os.O_CREATE | os.O_WRONLY
	if overwrite {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_APPEND
	}
	return os.OpenFile(path, flags, 0o644)
}

func main() {
	var (
		lineLen    = flag.Int("line-length", 80, "max characters for the random message portion")
		stdoutRate = flag.Float64("stdout-rate", 0, "stdout lines per second (0 disables)")
		stderrRate = flag.Float64("stderr-rate", 0, "stderr lines per second (0 disables)")
		stdoutFile = flag.String("stdout-file", "", "if set, write stdout stream to this file (default: process stdout)")
		stderrFile = flag.String("stderr-file", "", "if set, write stderr stream to this file (default: process stderr)")
		overwrite  = flag.Bool("overwrite", false, "if set, truncate output files instead of appending")
	)
	flag.Parse()

	if *lineLen < 0 {
		fmt.Fprintln(os.Stderr, "Error: --line-length must be >= 0")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	outW := io.Writer(os.Stdout)
	errW := io.Writer(os.Stderr)
	var outF, errF *os.File
	var err error

	if *stdoutFile != "" {
		outF, err = openOutFile(*stdoutFile, *overwrite)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to open --stdout-file %q: %v\n", *stdoutFile, err)
			os.Exit(2)
		}
		outW = outF
	}
	if *stderrFile != "" {
		errF, err = openOutFile(*stderrFile, *overwrite)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to open --stderr-file %q: %v\n", *stderrFile, err)
			_ = outF.Close()
			os.Exit(2)
		}
		errW = errF
	}
	if outF != nil {
		defer func() { _ = outF.Close() }()
	}
	if errF != nil {
		defer func() { _ = errF.Close() }()
	}

	seed := time.Now().UnixNano()
	r1 := rand.New(rand.NewSource(seed))
	r2 := rand.New(rand.NewSource(seed + 1))

	go runSpammer(ctx, outW, *stdoutRate, *lineLen, r1)
	go runSpammer(ctx, errW, *stderrRate, *lineLen, r2)

	<-ctx.Done()
}
