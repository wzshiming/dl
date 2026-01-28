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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultChunkSize is the default size of each download chunk (100MB).
const DefaultChunkSize = 100 * 1024 * 1024

// DefaultConcurrency is the default number of concurrent download workers.
const DefaultConcurrency = 4

const DefaultRetryPerHost = 2

const tmpDirPrefix = ".dl-"

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
	start    int64
	end      int64
	partFile string // path to the chunk part file
	existing atomic.Bool
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
	partFile := entireFilePath(outputPath, info)
	var lastErr error
	for _, url := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			continue
		}

		// Check if the file already exists and determine the range to resume
		var existingSize int64
		if stat, err := os.Stat(partFile); err == nil {
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
			file, err = os.OpenFile(partFile, os.O_APPEND|os.O_WRONLY, 0644)
		} else {
			file, err = os.Create(partFile)
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

		err = os.Rename(partFile, outputPath)
		if err != nil {
			return fmt.Errorf("failed to rename part file: %w", err)
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
	existingChunks := d.discoverExistingChunks(outputPath, info)

	// Progress tracking
	var downloadedBytes atomic.Int64
	var workersDownloadBytes []atomic.Int64

	if progressFunc != nil {
		for _, c := range existingChunks {
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

	// Calculate pending chunks based on existing downloaded chunks (supports dynamic sizes)
	chunks := d.calculatehunks(outputPath, info, existingChunks)

	// Create a channel for pending chunks
	chunkCh := make(chan *chunk, d.concurrency)
	go func() {
		for _, c := range chunks {
			if c.existing.Load() {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case chunkCh <- c:
			}
		}
		close(chunkCh)
	}()

	// Create error channel
	errCh := make(chan error, 1)

	// Channel to notify main thread when a chunk is completed
	chunkCompletedCh := make(chan struct{}, 1)

	// Create a context that can be canceled on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	reportCh := make(chan struct{}, 1)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < d.concurrency; i++ {
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
					err := d.downloadChunkToFile(ctx, url, *c, chunkProgressFn)
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
					case errCh <- fmt.Errorf("failed to download chunk %d-%d from all mirrors", c.start, c.end):
						cancel()
					default:
					}
					return
				}

				if progressFunc != nil {
					downloadedBytes.Add(workersDownloadBytes[workerID].Swap(0))
				}

				c.existing.Store(true)

				select {
				case chunkCompletedCh <- struct{}{}:
				default:
				}
			}
		}(i)
	}

	var mergeWg sync.WaitGroup
	mergeWg.Add(1)
	chunkCompletedCh <- struct{}{}
	go func() {
		defer mergeWg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-chunkCompletedCh:
				if !ok {
					return
				}

				d.tryMergeAdjacentChunks(chunks)
			}
		}
	}()

	// Wait for all workers to complete
	wg.Wait()

	// Close the completion channel to signal merge goroutine to finish
	close(chunkCompletedCh)

	// Wait for merge goroutine to finish
	mergeWg.Wait()

	// Check for errors
	select {
	case err := <-errCh:
		return err
	default:
	}

	firstChunk := chunks[0]
	fi, err := os.Stat(firstChunk.partFile)
	if err != nil {
		return fmt.Errorf("failed to stat final part file: %w", err)
	}
	if fi.Size() != info.size {
		return fmt.Errorf("final file size mismatch: expected %d, got %d", info.size, fi.Size())
	}

	err = os.Rename(firstChunk.partFile, outputPath)
	if err != nil {
		return fmt.Errorf("failed to rename final file: %w", err)
	}
	_ = CleanupPartFiles(outputPath)
	return nil
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

// parseChunkOffset extracts the offset from a chunk part file name.
// Returns -1 if the file name does not match the expected pattern.
func parseChunkOffset(name string, info *fileInfo) int64 {
	suffix := "-" + tempFileName(info)
	if !strings.HasSuffix(name, suffix) {
		return -1
	}
	name = strings.TrimSuffix(name, suffix)
	if !strings.HasPrefix(name, "offset-") {
		return -1
	}
	offset, err := strconv.ParseUint(name[7:], 10, 64)
	if err != nil {
		return -1
	}
	return int64(offset)
}

// discoverExistingChunks scans the download directory for existing chunk files.
// It returns chunks with their actual sizes as discovered from the file system.
func (d *Downloader) discoverExistingChunks(outputPath string, info *fileInfo) []*chunk {
	dir := downloadPath(outputPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var chunks []*chunk
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		offset := parseChunkOffset(name, info)
		if offset < 0 {
			continue
		}
		fi, err := entry.Info()
		if err != nil || fi.Size() == 0 {
			continue
		}
		end := offset + fi.Size() - 1
		if end >= info.size {
			end = info.size - 1
		}
		existing := atomic.Bool{}
		existing.Store(true)
		chunks = append(chunks, &chunk{
			start:    offset,
			end:      end,
			existing: existing,
			partFile: filepath.Join(dir, name),
		})
	}

	// Sort by start offset
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].start < chunks[j].start
	})

	return chunks
}

// calculatehunks calculates the list of chunks to download based on existing chunks.
func (d *Downloader) calculatehunks(outputPath string, info *fileInfo, existing []*chunk) (chunks []*chunk) {
	// Build list of covered ranges from existing chunks
	var coveredEnd int64
	for _, c := range existing {
		if c.start <= coveredEnd && c.end >= coveredEnd {
			// This chunk extends our covered range
			coveredEnd = c.end + 1
			chunks = append(chunks, c)
		} else if c.start > coveredEnd {
			// There's a gap before this chunk - need to download it
			// But first, add chunks for the gap
			chunks = append(chunks, d.chunksForRange(outputPath, info, coveredEnd, c.start-1)...)
			// This existing chunk is still valid
			coveredEnd = c.end + 1
			chunks = append(chunks, c)
		}
		// If c.end < coveredEnd, this chunk is already covered (redundant)
	}

	// Add chunks for any remaining range after the last existing chunk
	if coveredEnd < info.size {
		chunks = append(chunks, d.chunksForRange(outputPath, info, coveredEnd, info.size-1)...)
	}

	return chunks
}

// chunksForRange creates chunks to cover a range [start, end].
func (d *Downloader) chunksForRange(outputPath string, info *fileInfo, start, end int64) []*chunk {
	var chunks []*chunk
	chunkSize := d.chunkSize

	for offset := start; offset <= end; offset += chunkSize {
		chunkEnd := offset + chunkSize - 1
		if chunkEnd > end {
			chunkEnd = end
		}
		chunks = append(chunks, &chunk{
			start:    offset,
			end:      chunkEnd,
			partFile: chunkPartPath(outputPath, info, offset),
		})
	}

	return chunks
}

// downloadChunkToFile downloads a single chunk to its part file.
// It supports resuming from a partially downloaded file.
func (d *Downloader) downloadChunkToFile(ctx context.Context, url string, c chunk, progressFn ProgressFunc) error {
	expectedSize := c.end - c.start + 1

	// Check if there's a partial download we can resume from
	var existingSize int64
	if stat, err := os.Stat(c.partFile); err == nil {
		existingSize = stat.Size()
		// If the file is already complete, we're done
		if existingSize == expectedSize {
			if progressFn != nil {
				progressFn(expectedSize, expectedSize)
			}
			return nil
		}
		// If file is larger than expected, remove it and start fresh
		if existingSize > expectedSize {
			_ = os.Remove(c.partFile)
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
		file, err = os.OpenFile(c.partFile, os.O_APPEND|os.O_WRONLY, 0644)
	} else {
		file, err = os.Create(c.partFile)
	}
	if err != nil {
		return fmt.Errorf("failed to open/create part file: %w", err)
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("failed to download chunk: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close part file: %w", err)
	}

	return nil
}

// tryMergeAdjacentChunks tries to merge chunks into the first chunk (offset 0).
func (d *Downloader) tryMergeAdjacentChunks(chunks []*chunk) {
	if len(chunks) == 0 {
		return
	}

	first := chunks[0]
	if first.start != 0 || !first.existing.Load() {
		return
	}

	for i := 1; i < len(chunks); i++ {
		next := chunks[i]
		if next == nil {
			continue
		}
		if !next.existing.Load() {
			return
		}
		if next.start > first.end+1 {
			return
		}
		// Merge next into first
		err := d.mergeChunkFiles(first.partFile, next.partFile, next.start)
		if err != nil {
			return
		}
		first.end = next.end

		// Remove merged chunk from list
		chunks[i] = nil

		continue
	}
}

// mergeChunkFiles appends the content of srcFile to dstFile.
func (d *Downloader) mergeChunkFiles(dstFile, srcFile string, offset int64) error {
	src, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(dstFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer dst.Close()

	dstStat, err := dst.Stat()
	if err != nil {
		return err
	}
	dstSize := dstStat.Size()
	if dstSize < offset {
		return fmt.Errorf("destination file size %d is less than expected offset %d", dstSize, offset)
	} else if dstSize > offset {
		_, err = src.Seek(dstStat.Size()-offset, io.SeekStart)
		if err != nil {
			return err
		}
	}

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}

	err = dst.Sync()
	if err != nil {
		return err
	}

	err = os.Remove(srcFile)
	if err != nil {
		return err
	}
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
