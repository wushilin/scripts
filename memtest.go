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
		initialStr  = flag.String("initial-size", "10M", "initial bytes to allocate (e.g. 10M, 512KiB). K/M/G are powers-of-2")
		increaseStr = flag.String("increase-by", "10M", "increase allocation by this amount each interval (default 10M)")
		maxStr      = flag.String("max-size", "4G", "maximum allocation size (default 4G)")
		intervalMS  = flag.Int("intervalms", 10000, "allocation step interval (ms). each interval, re-allocate at the next size (default 10000)")
		sleepMS     = flag.Int("sleepms", 100, "sleep duration (ms) between passes (default 100)")
		reportStr   = flag.String("report", "1G", "report every N bytes written (default 1G)")
	)
	flag.Parse()

	if *sleepMS < 0 {
		fmt.Fprintln(os.Stderr, "Error: --sleepms must be >= 0")
		os.Exit(2)
	}
	if *intervalMS <= 0 {
		fmt.Fprintln(os.Stderr, "Error: --intervalms must be > 0")
		os.Exit(2)
	}

	initialBytes, err := parseBytesPow2(*initialStr)
	if err != nil || initialBytes == 0 {
		fmt.Fprintf(os.Stderr, "Error: invalid --initial-size %q: %v\n", *initialStr, err)
		os.Exit(2)
	}
	increaseBy, err := parseBytesPow2(*increaseStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid --increase-by %q: %v\n", *increaseStr, err)
		os.Exit(2)
	}
	maxBytes, err := parseBytesPow2(*maxStr)
	if err != nil || maxBytes == 0 {
		fmt.Fprintf(os.Stderr, "Error: invalid --max-size %q: %v\n", *maxStr, err)
		os.Exit(2)
	}
	if initialBytes > maxBytes {
		initialBytes = maxBytes
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
	sleepDur := time.Duration(*sleepMS) * time.Millisecond
	stepDur := time.Duration(*intervalMS) * time.Millisecond

	var total uint64
	nextReport := reportEvery

	seed := time.Now().UnixNano()
	r := rand.New(rand.NewSource(seed))
	scratch := make([]byte, 4*1024)

	curSize := initialBytes

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if curSize > maxBytes {
			curSize = maxBytes
		}
		if curSize > uint64(maxInt) {
			fmt.Fprintf(os.Stderr, "Error: size too large for this platform: %s (%d bytes)\n", fmtBytesIEC(curSize), curSize)
			os.Exit(2)
		}

		buf, allocErr := tryAlloc(int(curSize))
		if allocErr != nil {
			fmt.Fprintf(os.Stderr, "Error: allocation failed for size %s (%d bytes): %v\n", fmtBytesIEC(curSize), curSize, allocErr)
			os.Exit(2)
		}
		fmt.Printf("step: allocated %s memory (%d bytes)\n", fmtBytesIEC(curSize), curSize)

		stepEnd := time.Now().Add(stepDur)

		// Within this step window, run fill/sleep passes until step ends.
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if time.Now().After(stepEnd) {
				break
			}

			// Fill buffer using 4KiB chunks. After each chunk, check if the step is over.
			for off := 0; off < len(buf); off += len(scratch) {
				if time.Now().After(stepEnd) {
					break
				}
				n := len(scratch)
				if off+n > len(buf) {
					n = len(buf) - off
				}
				_, _ = r.Read(scratch[:n])
				copy(buf[off:off+n], scratch[:n])
				total += uint64(n)

				for total >= nextReport {
					fmt.Printf("written %s (%d bytes)\n", fmtBytesIEC(total), total)
					nextReport += reportEvery
				}
			}

			// Sleep between passes, but don't oversleep past the step boundary.
			if sleepDur > 0 {
				remain := time.Until(stepEnd)
				if remain <= 0 {
					break
				}
				d := sleepDur
				if d > remain {
					d = remain
				}
				timer := time.NewTimer(d)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}

		// Drop previous allocation, then update size for next step.
		buf = nil
		if curSize < maxBytes && increaseBy > 0 {
			next := curSize + increaseBy
			if next > maxBytes {
				next = maxBytes
			}
			if next != curSize {
				fmt.Printf("step: stepping up to %s memory (%d bytes)\n", fmtBytesIEC(next), next)
				curSize = next
			}
		} else {
			// Either at max already or increase disabled
			fmt.Printf("step: staying at %s memory (%d bytes)\n", fmtBytesIEC(curSize), curSize)
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
