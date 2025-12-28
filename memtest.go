package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func fmtBytesIEC(n uint64) string {
	const (
		kiB = 1 << 10
		miB = 1 << 20
		giB = 1 << 30
		tiB = 1 << 40
	)
	switch {
	case n >= tiB:
		return fmt.Sprintf("%.2fTiB", float64(n)/float64(tiB))
	case n >= giB:
		return fmt.Sprintf("%.2fGiB", float64(n)/float64(giB))
	case n >= miB:
		return fmt.Sprintf("%.2fMiB", float64(n)/float64(miB))
	case n >= kiB:
		return fmt.Sprintf("%.2fKiB", float64(n)/float64(kiB))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func parseBytesPow2(s string) (uint64, error) {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return 0, fmt.Errorf("empty size")
	}

	// Split numeric prefix and suffix.
	i := 0
	for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("missing number in %q", s)
	}
	numStr := raw[:i]
	suf := strings.TrimSpace(raw[i:])
	suf = strings.ToLower(suf)

	n, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %w", numStr, err)
	}

	mul := uint64(1)
	switch suf {
	case "", "b":
		mul = 1
	case "k", "kb", "kib":
		mul = 1 << 10
	case "m", "mb", "mib":
		mul = 1 << 20
	case "g", "gb", "gib":
		mul = 1 << 30
	default:
		return 0, fmt.Errorf("unknown unit %q (supported: K/M/G, KiB/MiB/GiB)", suf)
	}

	// Overflow check: n * mul <= max uint64
	if mul != 0 && n > (^uint64(0))/mul {
		return 0, fmt.Errorf("size too large: %q", s)
	}
	return n * mul, nil
}

func main() {
	var (
		sizeStr   = flag.String("size", "", "required. bytes to allocate (e.g. 12M, 512KiB). K/M/G are powers-of-2")
		sleepMS   = flag.Int("sleepms", 100, "sleep duration (ms) between passes")
		reportStr = flag.String("report", "1G", "report every N bytes written (default 1G)")
	)
	flag.Parse()

	if *sizeStr == "" {
		fmt.Fprintln(os.Stderr, "Error: --size is required")
		os.Exit(2)
	}
	if *sleepMS < 0 {
		fmt.Fprintln(os.Stderr, "Error: --sleepms must be >= 0")
		os.Exit(2)
	}

	sizeBytes, err := parseBytesPow2(*sizeStr)
	if err != nil || sizeBytes == 0 {
		fmt.Fprintf(os.Stderr, "Error: invalid --size %q: %v\n", *sizeStr, err)
		os.Exit(2)
	}
	reportEvery, err := parseBytesPow2(*reportStr)
	if err != nil || reportEvery == 0 {
		fmt.Fprintf(os.Stderr, "Error: invalid --report %q: %v\n", *reportStr, err)
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Ensure size fits into an int for make([]byte, int(...)) on this platform.
	maxInt := int(^uint(0) >> 1)
	if sizeBytes > uint64(maxInt) {
		fmt.Fprintf(os.Stderr, "Error: --size %s (%d bytes) is too large for this platform\n", fmtBytesIEC(sizeBytes), sizeBytes)
		os.Exit(2)
	}

	buf, allocErr := tryAlloc(int(sizeBytes))
	if allocErr != nil {
		fmt.Fprintf(os.Stderr, "Error: allocation failed for --size %s (%d bytes): %v\n", fmtBytesIEC(sizeBytes), sizeBytes, allocErr)
		os.Exit(2)
	}
	sleepDur := time.Duration(*sleepMS) * time.Millisecond

	var total uint64
	nextReport := reportEvery

	seed := time.Now().UnixNano()
	r := rand.New(rand.NewSource(seed))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Fill with random bytes.
		_, _ = r.Read(buf)
		total += uint64(len(buf))

		for total >= nextReport {
			fmt.Printf("written %s (%d bytes)\n", fmtBytesIEC(total), total)
			nextReport += reportEvery
		}

		if sleepDur > 0 {
			timer := time.NewTimer(sleepDur)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func tryAlloc(n int) (_ []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	return make([]byte, n), nil
}
