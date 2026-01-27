package dl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultChunkSize is the default size of each download chunk (100MB).
const DefaultChunkSize = 10 * 1024 * 1024

// DefaultConcurrency is the default number of concurrent download workers.
const DefaultConcurrency = 4

const DefaultRetryPerHost = 2

const tmpDirPrefix = ".dl-"

const tmpFileSuffix = ".tmp"

// ErrNoMirrors is returned when no mirror URLs are provided.
var ErrNoMirrors = errors.New("no mirror URLs provided")

// Downloader handles multi-threaded concurrent file downloads.
type Downloader struct {
	// httpClient is the HTTP client used for requests.
	httpClient *http.Client

	// chunkSize is the size of each download chunk in bytes.
	chunkSize int64

	// concurrency is the number of concurrent download workers.
	concurrency int

	// MaxRetry is the maximum number of retries for failed chunk downloads.
	retryPerHost int

	// forceTryRange forces chunked download even if server doesn't advertise range support.
	forceTryRange bool
}

type Option func(*Downloader)

func WithHTTPClient(client *http.Client) Option {
	return func(d *Downloader) {
		d.httpClient = client
	}
}

func WithChunkSize(size int64) Option {
	return func(d *Downloader) {
		d.chunkSize = size
	}
}

func WithConcurrency(concurrency int) Option {
	return func(d *Downloader) {
		d.concurrency = concurrency
	}
}

func WithRetryPerHost(retry int) Option {
	return func(d *Downloader) {
		d.retryPerHost = retry
	}
}

func WithForceTryRange(force bool) Option {
	return func(d *Downloader) {
		d.forceTryRange = force
	}
}

// NewDownloader creates a new Downloader with default settings.
func NewDownloader(opts ...Option) *Downloader {
	d := &Downloader{
		httpClient:    http.DefaultClient,
		chunkSize:     DefaultChunkSize,
		concurrency:   DefaultConcurrency,
		retryPerHost:  DefaultRetryPerHost,
		forceTryRange: true,
	}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

// chunk represents a download chunk.
type chunk struct {
	index    int
	start    int64
	end      int64
	partFile string // path to the chunk part file
}

// ProgressFunc is a callback function for reporting download progress.
type ProgressFunc func(downloaded, total int64)

// Download downloads a file with progress reporting.
func (d *Downloader) Download(ctx context.Context, outputPath string, progressFunc ProgressFunc, urls ...string) error {
	if len(urls) == 0 {
		return ErrNoMirrors
	}

	// Get file information from the first available mirror
	fileInfo, err := d.getFileInfo(ctx, urls)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Check if file is already complete
	if stat, err := os.Stat(outputPath); err == nil {
		if stat.Size() == fileInfo.size && fileInfo.size > 0 {
			if progressFunc != nil {
				progressFunc(fileInfo.size, fileInfo.size)
			}
			return nil
		}
	}

	// Ensure output directory exists
	if err := os.MkdirAll(downloadPath(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	if !fileInfo.supportsRange && !d.forceTryRange {
		return d.downloadDirect(ctx, outputPath, urls, fileInfo, progressFunc)
	}

	if fileInfo.size <= d.chunkSize {
		return d.downloadDirect(ctx, outputPath, urls, fileInfo, progressFunc)
	}

	// Chunked concurrent download with resume support
	return d.downloadChunked(ctx, outputPath, urls, fileInfo, progressFunc)
}

// fileInfo contains information about the remote file.
type fileInfo struct {
	size          int64
	etag          string
	supportsRange bool
}

// getFileInfo retrieves file information from the first available mirror.
func (d *Downloader) getFileInfo(ctx context.Context, urls []string) (*fileInfo, error) {
	var lastErr error
	for _, url := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := d.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			continue
		}

		supportsRange := resp.Header.Get("Accept-Ranges") == "bytes"
		return &fileInfo{
			size:          resp.ContentLength,
			etag:          resp.Header.Get("ETag"),
			supportsRange: supportsRange,
		}, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("failed to get file info from all mirrors")
}

// downloadDirect downloads a file without chunking (fallback for small files or servers without range support).
func (d *Downloader) downloadDirect(ctx context.Context, outputPath string, urls []string, info *fileInfo, progressFunc ProgressFunc) error {
	tmpFile := entireFilePath(outputPath, info) + tmpFileSuffix
	var lastErr error
	for _, url := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			continue
		}

		// Check if the file already exists and determine the range to resume
		var existingSize int64
		if stat, err := os.Stat(tmpFile); err == nil {
			existingSize = stat.Size()
			if existingSize > 0 {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-", existingSize))
			}
		}

		resp, err := d.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			continue
		}

		// Open the file for appending if resuming, otherwise create a new file
		var file *os.File
		if existingSize > 0 {
			file, err = os.OpenFile(tmpFile, os.O_APPEND|os.O_WRONLY, 0644)
		} else {
			file, err = os.Create(tmpFile)
		}
		if err != nil {
			resp.Body.Close()
			return fmt.Errorf("failed to open/create output file: %w", err)
		}

		var reader io.Reader = resp.Body
		if progressFunc != nil {
			reader = &progressReader{
				ctx:          ctx,
				reader:       resp.Body,
				total:        resp.ContentLength + existingSize,
				read:         existingSize,
				progressFunc: progressFunc,
			}
		}

		_, err = io.Copy(file, reader)
		_ = file.Close()
		_ = resp.Body.Close()

		if err != nil {
			lastErr = err
			continue
		}

		err = os.Rename(tmpFile, outputPath)
		if err != nil {
			return fmt.Errorf("failed to rename temp file: %w", err)
		}

		_ = CleanupPartFiles(outputPath)
		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return errors.New("failed to download from all mirrors")
}

// downloadChunked performs a chunked concurrent download with resume support.
// Each chunk is downloaded to a separate part file, then merged when complete.
func (d *Downloader) downloadChunked(ctx context.Context, outputPath string, urls []string, info *fileInfo, progressFunc ProgressFunc) error {

	// Calculate all chunks and their part file paths
	allChunks := d.calculateChunks(outputPath, info)
	if len(allChunks) == 0 {
		return nil
	}

	// Find which chunks still need to be downloaded (resume support)
	pendingChunks, completedChunks := d.findPendingChunks(allChunks)

	// If all chunks are complete, merge them
	if len(pendingChunks) == 0 {
		if progressFunc != nil {
			progressFunc(info.size, info.size)
		}
		return d.mergeChunks(outputPath, allChunks)
	}

	// Create a channel for pending chunks
	chunkCh := make(chan chunk, len(pendingChunks))
	for _, c := range pendingChunks {
		chunkCh <- c
	}
	close(chunkCh)

	// Create error channel
	errCh := make(chan error, d.concurrency)

	// Create a context that can be canceled on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Progress tracking
	var downloadedBytes atomic.Int64
	var workersDownloadBytes []atomic.Int64

	reportCh := make(chan struct{}, 1)

	if progressFunc != nil {
		for _, c := range completedChunks {
			downloadedBytes.Add(c.end - c.start + 1)
		}
		workersDownloadBytes = make([]atomic.Int64, d.concurrency)
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					totalDownloaded := downloadedBytes.Load()
					for i := range workersDownloadBytes {
						totalDownloaded += workersDownloadBytes[i].Load()
					}
					progressFunc(totalDownloaded, info.size)
				}
			}
		}()
	}

	concurrency := min(d.concurrency, len(pendingChunks))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			var chunkProgressFn ProgressFunc
			if progressFunc != nil {
				chunkProgressFn = func(downloaded, total int64) {
					workersDownloadBytes[workerID].Store(downloaded)
					select {
					case reportCh <- struct{}{}:
					default:
					}
				}
			}

			mirrorIdx := workerID % len(urls)
			for c := range chunkCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Try all mirrors starting from assigned one
				downloaded := false
				for attempt := 0; attempt < len(urls)*d.retryPerHost; attempt++ {
					url := urls[(mirrorIdx+attempt)%len(urls)]
					err := d.downloadChunkToFile(ctx, url, c, chunkProgressFn)
					if err == nil {
						downloaded = true
						break
					}
					if errors.Is(err, context.Canceled) {
						break
					}
				}

				if !downloaded {
					select {
					case errCh <- fmt.Errorf("failed to download chunk %d from all mirrors", c.index):
						cancel()
					default:
					}
					return
				}

				if progressFunc != nil {
					downloadedBytes.Add(workersDownloadBytes[workerID].Swap(0))
				}
			}
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()

	// Check for errors
	select {
	case err := <-errCh:
		return err
	default:
	}

	// All chunks downloaded, merge them
	return d.mergeChunks(outputPath, allChunks)
}

func downloadPath(outputPath string) string {
	return filepath.Join(filepath.Dir(outputPath), tmpDirPrefix+filepath.Base(outputPath))
}

func tempFileName(info *fileInfo) string {
	if info.etag != "" {
		return fmt.Sprintf("etag-%s", normalizeEtag(info.etag))
	}
	if info.size > 0 {
		return fmt.Sprintf("size-%d", info.size)
	}
	return "unknown"
}

func normalizeEtag(etag string) string {
	etag = strings.TrimPrefix(etag, "W/")
	etag = strings.Trim(etag, "\"")
	return etag
}

func entireFilePath(outputPath string, info *fileInfo) string {
	return fmt.Sprintf("%s/entire-%s", downloadPath(outputPath), tempFileName(info))
}

// chunkPartPath returns the path to a chunk part file.
func chunkPartPath(outputPath string, info *fileInfo, start int64) string {
	return fmt.Sprintf("%s/offset-%d-%s", downloadPath(outputPath), start, tempFileName(info))
}

// calculateChunks calculates all chunks needed for the download.
func (d *Downloader) calculateChunks(outputPath string, info *fileInfo) []chunk {
	var chunks []chunk
	chunkSize := d.chunkSize
	index := 0

	for offset := int64(0); offset < info.size; offset += chunkSize {
		end := offset + chunkSize - 1
		if end >= info.size {
			end = info.size - 1
		}

		chunks = append(chunks, chunk{
			index:    index,
			start:    offset,
			end:      end,
			partFile: chunkPartPath(outputPath, info, offset),
		})
		index++
	}

	return chunks
}

// findPendingChunks finds chunks that still need to be downloaded.
// Returns the pending chunks and the total bytes already downloaded (including partial temp files).
func (d *Downloader) findPendingChunks(chunks []chunk) (pending, completed []chunk) {
	for _, c := range chunks {
		expectedSize := c.end - c.start + 1
		stat, err := os.Stat(c.partFile)
		if err == nil && stat.Size() == expectedSize {
			// Chunk is complete
			completed = append(completed, c)
		} else {

			pending = append(pending, c)
		}
	}

	return pending, completed
}

// downloadChunkToFile downloads a single chunk to its part file.
// It supports resuming from a partially downloaded temp file.
func (d *Downloader) downloadChunkToFile(ctx context.Context, url string, c chunk, progressFn ProgressFunc) error {
	tmpFile := c.partFile + tmpFileSuffix
	expectedSize := c.end - c.start + 1

	// Check if there's a partial download we can resume from
	var existingSize int64
	if stat, err := os.Stat(tmpFile); err == nil {
		existingSize = stat.Size()
		// If the temp file is already complete, just rename it
		if existingSize == expectedSize {
			if err := os.Rename(tmpFile, c.partFile); err != nil {
				return err
			}
			if progressFn != nil {
				progressFn(expectedSize, expectedSize)
			}
			return nil
		}
		// If temp file is larger than expected, remove it and start fresh
		if existingSize > expectedSize {
			os.Remove(tmpFile)
			existingSize = 0
		}
	}

	// Calculate the actual range to download
	rangeStart := c.start + existingSize
	rangeEnd := c.end

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd))

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status code: %d (expected 206)", resp.StatusCode)
	}

	var reader io.Reader = resp.Body
	if progressFn != nil {
		reader = &progressReader{
			ctx:          ctx,
			reader:       resp.Body,
			total:        resp.ContentLength + existingSize,
			read:         existingSize,
			progressFunc: progressFn,
		}
	}

	// Open file for appending if resuming, otherwise create new
	var file *os.File
	if existingSize > 0 {
		file, err = os.OpenFile(tmpFile, os.O_APPEND|os.O_WRONLY, 0644)
	} else {
		file, err = os.Create(tmpFile)
	}
	if err != nil {
		return fmt.Errorf("failed to open/create temp file: %w", err)
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("failed to download chunk: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpFile, c.partFile); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// mergeChunks merges all chunk part files into the final output file.
func (d *Downloader) mergeChunks(outputPath string, chunks []chunk) error {
	// Sort chunks by index to ensure correct order
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].index < chunks[j].index
	})

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Merge each chunk
	for _, c := range chunks {
		partFile, err := os.Open(c.partFile)
		if err != nil {
			return fmt.Errorf("failed to open part file %s: %w", c.partFile, err)
		}

		_, err = io.Copy(outFile, partFile)
		_ = partFile.Close()
		if err != nil {
			return fmt.Errorf("failed to copy part file %s: %w", c.partFile, err)
		}
	}

	_ = CleanupPartFiles(outputPath)
	return nil
}

// CleanupPartFiles removes any leftover part files for a download.
func CleanupPartFiles(outputPath string) error {
	return os.RemoveAll(downloadPath(outputPath))
}

// progressReader wraps an io.Reader to report progress.
type progressReader struct {
	ctx          context.Context
	reader       io.Reader
	total        int64
	read         int64
	progressFunc ProgressFunc
}

func (pr *progressReader) Read(p []byte) (int, error) {
	if pr.ctx.Err() != nil {
		return 0, pr.ctx.Err()
	}

	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.read += int64(n)
		if pr.progressFunc != nil {
			pr.progressFunc(pr.read, pr.total)
		}
	}
	return n, err
}
