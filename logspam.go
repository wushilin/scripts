package main

import (
	"context"
	"flag"
	"fmt"
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

func runSpammer(ctx context.Context, w *os.File, rate float64, lineLen int, r *rand.Rand) {
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

func main() {
	var (
		lineLen    = flag.Int("line-length", 80, "max characters for the random message portion")
		stdoutRate = flag.Float64("stdout-rate", 0, "stdout lines per second (0 disables)")
		stderrRate = flag.Float64("stderr-rate", 0, "stderr lines per second (0 disables)")
	)
	flag.Parse()

	if *lineLen < 0 {
		fmt.Fprintln(os.Stderr, "Error: --line-length must be >= 0")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	seed := time.Now().UnixNano()
	r1 := rand.New(rand.NewSource(seed))
	r2 := rand.New(rand.NewSource(seed + 1))

	go runSpammer(ctx, os.Stdout, *stdoutRate, *lineLen, r1)
	go runSpammer(ctx, os.Stderr, *stderrRate, *lineLen, r2)

	<-ctx.Done()
}
