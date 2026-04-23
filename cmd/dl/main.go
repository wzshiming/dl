package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/wzshiming/dl"
)

var (
	output       string
	concurrency  int
	chunkSize    int64
	quiet        bool
	retryPerHost int
	resume       bool
)

func init() {
	flag.StringVar(&output, "o", "", "Output file path (default: derived from URL)")
	flag.IntVar(&concurrency, "c", dl.DefaultConcurrency, "Number of concurrent connections")
	flag.Int64Var(&chunkSize, "chunk-size", dl.DefaultChunkSize, "Size of each download chunk in bytes")
	flag.IntVar(&retryPerHost, "r", dl.DefaultRetryPerHost, "Number of retries per host on failure")
	flag.BoolVar(&resume, "resume", false, "Resume download from existing output file")
	flag.BoolVar(&quiet, "q", false, "Quiet mode (no progress output)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <url> [mirror_url...]\n\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "A multi-threaded concurrent file downloader with support for:")
		fmt.Fprintln(os.Stderr, "  - Automatic chunked downloads")
		fmt.Fprintln(os.Stderr, "  - Multiple mirror sources for redundancy")
		fmt.Fprintln(os.Stderr, "  - Resume capability for incomplete downloads")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Options:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Examples:")
		fmt.Fprintln(os.Stderr, "  dl https://example.com/file.zip")
		fmt.Fprintln(os.Stderr, "  dl -o myfile.zip https://example.com/file.zip")
		fmt.Fprintln(os.Stderr, "  dl -c 8 https://example.com/file.zip https://mirror.example.com/file.zip")
	}
	flag.Parse()
}

func main() {
	urls := flag.Args()
	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "Error: at least one URL is required")
		flag.Usage()
		os.Exit(1)
	}

	// Derive output filename from URL if not specified
	if output == "" {
		output = deriveFilename(urls[0])
		if output == "" {
			fmt.Fprintln(os.Stderr, "Error: could not derive output filename from URL, please specify with -o")
			os.Exit(1)
		}
	}

	// Setup context with cancellation on interrupt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nDownload interrupted. Run again to resume.")
		cancel()
	}()

	// Progress callback
	var progressFunc dl.ProgressFunc
	if !quiet {
		fmt.Printf("Downloading to: %s\n", output)
		fmt.Printf("Using %d concurrent\n", concurrency)
		fmt.Printf("URLs:\n")
		for _, url := range urls {
			fmt.Printf("  %s\n", url)
		}
		fmt.Println()

		var lastDownloaded int64
		var lastTime = time.Now()
		var speed float64

		progressFunc = func(name string, downloaded, total int64) {
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			if lastDownloaded == 0 {
				lastDownloaded = downloaded
			}
			if elapsed > 5 || downloaded == total {
				speed = float64(downloaded-lastDownloaded) / elapsed
				lastDownloaded = downloaded
				lastTime = now

				if total > 0 {
					percent := float64(downloaded) / float64(total) * 100
					fmt.Printf("Progress: %.1f%% (%s / %s) - Speed: %s/s\t\r", percent, formatBytes(downloaded), formatBytes(total), formatBytes(int64(speed)))
				}
			}
		}
	}

	d := dl.NewDownloader(
		dl.WithConcurrency(concurrency),
		dl.WithChunkSize(chunkSize),
		dl.WithRetryPerHost(retryPerHost),
		dl.WithForceTryRange(true),
		dl.WithResume(resume),
		dl.WithProgressFunc(progressFunc),
	)

	// Open output file for writing
	outputFile, err := os.OpenFile(output, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to open output file: %v\n", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	// Start download
	err = d.Download(ctx, output, outputFile, urls...)
	if err != nil {
		if ctx.Err() != nil {
			os.Exit(130) // Standard exit code for SIGINT
		}
		fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
		os.Exit(1)
	}

	if !quiet {
		fmt.Println("\nDownload complete!")
	}
}

// deriveFilename extracts a filename from a URL.
func deriveFilename(url string) string {
	// Remove query string and fragment
	if idx := strings.Index(url, "?"); idx != -1 {
		url = url[:idx]
	}
	if idx := strings.Index(url, "#"); idx != -1 {
		url = url[:idx]
	}

	// Get the last path component
	parts := strings.Split(url, "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] != "" {
			return filepath.Base(parts[i])
		}
	}

	return ""
}

// formatBytes formats a byte count in a human-readable format.
func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GiB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MiB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KiB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
