package dl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultChunkSize is the default size of each download chunk (64MB).
const DefaultChunkSize = 64 * 1024 * 1024

// DefaultConcurrency is the default number of concurrent download workers.
const DefaultConcurrency = 8

// DefaultRetryPerHost is the default number of retries per host for failed chunk downloads.
const DefaultRetryPerHost = 4

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

	// resume indicates whether to resume from existing output file.
	resume bool

	// progressFunc is the callback function for reporting download progress.
	progressFunc ProgressFunc

	// cacheDir is the directory used for caching file information (not implemented in this version).
	cacheDir string
}

// Option defines a functional option for configuring the Downloader.
type Option func(*Downloader)

// WithHTTPClient sets a custom HTTP client for the Downloader.
func WithHTTPClient(client *http.Client) Option {
	return func(d *Downloader) {
		d.httpClient = client
	}
}

// WithChunkSize sets the size of each download chunk in bytes.
func WithChunkSize(size int64) Option {
	return func(d *Downloader) {
		d.chunkSize = size
	}
}

// WithConcurrency sets the number of concurrent download workers.
func WithConcurrency(concurrency int) Option {
	return func(d *Downloader) {
		d.concurrency = concurrency
	}
}

// WithRetryPerHost sets the number of retries per host for failed chunk downloads.
func WithRetryPerHost(retry int) Option {
	return func(d *Downloader) {
		d.retryPerHost = retry
	}
}

// WithForceTryRange sets whether to force chunked download even if server doesn't advertise range support.
func WithForceTryRange(force bool) Option {
	return func(d *Downloader) {
		d.forceTryRange = force
	}
}

// WithResume sets whether to resume from existing output file.
func WithResume(resume bool) Option {
	return func(d *Downloader) {
		d.resume = resume
	}
}

// WithProgressFunc sets the progress reporting callback function.
func WithProgressFunc(progressFunc ProgressFunc) Option {
	return func(d *Downloader) {
		d.progressFunc = progressFunc
	}
}

// WithCacheDir sets the directory used for caching file information (not implemented in this version).
func WithCacheDir(cacheDir string) Option {
	return func(d *Downloader) {
		d.cacheDir = cacheDir
	}
}

// NewDownloader creates a new Downloader with default settings.
func NewDownloader(opts ...Option) *Downloader {
	d := &Downloader{
		httpClient:    http.DefaultClient,
		chunkSize:     DefaultChunkSize,
		concurrency:   DefaultConcurrency,
		retryPerHost:  DefaultRetryPerHost,
		forceTryRange: false,
		resume:        false,
	}

	for _, opt := range opts {
		opt(d)
	}

	if d.cacheDir == "" {
		d.cacheDir = path.Join(os.TempDir(), "dl-cache")
	}

	return d
}

type Writer interface {
	io.Seeker
	io.Writer
}

// chunk represents a download chunk.
type chunk struct {
	start    int64
	end      int64
	writer   Writer // offset 0: passed-in Writer; other offsets: *lazyWriter backed by a part file
	existing uint32
}

func (c *chunk) Existing() bool {
	return atomic.LoadUint32(&c.existing) != 0
}

func (c *chunk) SetExisting() {
	atomic.StoreUint32(&c.existing, 1)
}

// lazyWriter is a Writer backed by a file that is opened lazily on first Write.
// Seek(0, io.SeekEnd) returns the current file size (0 if the file does not exist yet).
// Seek(0, io.SeekStart) removes the file and resets state.
// After Redirect is called all writes go directly to the real output writer.
type lazyWriter struct {
	mu          sync.Mutex
	path        string
	file        *os.File
	real        Writer // non-nil after Redirect(); all writes go here
	startOffset int64  // absolute file offset of this chunk; used after Redirect
}

func newLazyWriter(path string, startOffset int64) *lazyWriter {
	return &lazyWriter{path: path, startOffset: startOffset}
}

// Path returns the underlying file path.
func (lw *lazyWriter) Path() string { return lw.path }

// IsRedirected reports whether this writer has been redirected to the real output writer.
func (lw *lazyWriter) IsRedirected() bool {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.real != nil
}

// Redirect merges any bytes already written to the lazy file into real, then redirects
// all future Write calls to real. The merge goroutine must ensure real is correctly
// positioned (at lw.startOffset) before calling Redirect.
func (lw *lazyWriter) Redirect(real Writer, mergeFunc func(Writer, string, int64) error) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if lw.real != nil {
		return nil // already redirected
	}
	if lw.file != nil {
		if err := lw.file.Close(); err != nil {
			return err
		}
		lw.file = nil
	}
	// Merge any bytes already written to the lazy file.
	if stat, err := os.Stat(lw.path); err == nil && stat.Size() > 0 {
		if err := mergeFunc(real, lw.path, lw.startOffset); err != nil {
			return err
		}
	}
	lw.real = real
	return nil
}

func (lw *lazyWriter) Seek(offset int64, whence int) (int64, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	switch {
	case whence == io.SeekEnd && offset == 0:
		if lw.real != nil {
			realSize, err := lw.real.Seek(0, io.SeekEnd)
			if err != nil {
				return 0, err
			}
			if realSize <= lw.startOffset {
				return 0, nil
			}
			return realSize - lw.startOffset, nil
		}
		if lw.file != nil {
			return lw.file.Seek(0, io.SeekEnd)
		}
		stat, err := os.Stat(lw.path)
		if err != nil {
			if os.IsNotExist(err) {
				return 0, nil
			}
			return 0, err
		}
		return stat.Size(), nil
	case whence == io.SeekStart && offset == 0:
		if lw.real != nil {
			// Reset after redirect: reposition real to startOffset.
			// This path should not occur in normal operation after redirect.
			_, err := lw.real.Seek(lw.startOffset, io.SeekStart)
			return 0, err
		}
		if lw.file != nil {
			_ = lw.file.Close()
			lw.file = nil
		}
		_ = os.Remove(lw.path)
		return 0, nil
	default:
		return 0, fmt.Errorf("lazyWriter: unsupported seek: offset=%d whence=%d", offset, whence)
	}
}

func (lw *lazyWriter) Write(p []byte) (int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if lw.real != nil {
		return lw.real.Write(p)
	}
	if lw.file == nil {
		var err error
		if stat, serr := os.Stat(lw.path); serr == nil && stat.Size() > 0 {
			lw.file, err = os.OpenFile(lw.path, os.O_APPEND|os.O_WRONLY, 0644)
		} else {
			lw.file, err = os.Create(lw.path)
		}
		if err != nil {
			return 0, fmt.Errorf("failed to open/create part file: %w", err)
		}
	}
	return lw.file.Write(p)
}

func (lw *lazyWriter) Close() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if lw.real != nil {
		return nil // real writer is managed externally
	}
	if lw.file != nil {
		err := lw.file.Close()
		lw.file = nil
		return err
	}
	return nil
}

// ProgressFunc is a callback function for reporting download progress.
type ProgressFunc func(name string, downloaded, total int64)

// Download downloads a file with progress reporting.
func (d *Downloader) Download(ctx context.Context, name string, writer Writer, urls ...string) error {
	if len(urls) == 0 {
		return ErrNoMirrors
	}

	// Get file information from the first available mirror
	info, err := d.getFileInfo(ctx, urls)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	currentSize, err := writer.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to get current size: %w", err)
	}

	if info.size > 0 {
		if currentSize > info.size {
			return fmt.Errorf("output file is larger than expected: %d > %d", currentSize, info.size)
		}

		if currentSize > 0 && currentSize < info.size {
			if !d.resume {
				return fmt.Errorf("output file is smaller than expected but resume is disabled: %d < %d", currentSize, info.size)
			}
		}

		if currentSize == info.size {
			return nil // File is already fully downloaded
		}
	}

	if !info.supportsRange && !d.forceTryRange {
		return d.downloadWholes(ctx, name, writer, info, urls)
	}

	if info.size <= d.chunkSize {
		return d.downloadWholes(ctx, name, writer, info, urls)
	}

	// Chunked concurrent download with resume support
	return d.downloadChunked(ctx, name, writer, info, urls)
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
			lastErr = fmt.Errorf("head request returned status code %d", resp.StatusCode)
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

func (d *Downloader) downloadWholes(ctx context.Context, name string, writer Writer, info *fileInfo, urls []string) error {
	var lastErr error
	for _, url := range urls {
		err := d.downloadWhole(ctx, name, writer, info, url)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		lastErr = err
	}
	return lastErr
}

func (d *Downloader) downloadWhole(ctx context.Context, name string, writer Writer, info *fileInfo, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: unexpected status code: %d (expected 200)", url, resp.StatusCode)
	}

	if resp.ContentLength > 0 && info.size > 0 && resp.ContentLength != info.size {
		return fmt.Errorf("%s: content length mismatch: expected %d, got %d", url, info.size, resp.ContentLength)
	}

	var reader io.Reader = resp.Body
	if d.progressFunc != nil {
		reader = &progressReader{
			ctx:    ctx,
			reader: resp.Body,
			total:  info.size,
			read:   0,
			progressFunc: func(downloaded, total int64) {
				d.progressFunc(name, downloaded, total)
			},
		}
	}

	_, err = writer.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek output: %w", err)
	}

	n, err := io.Copy(writer, reader)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	if info.size > 0 && n != info.size {
		return fmt.Errorf("downloaded size mismatch: expected %d, got %d", info.size, n)
	}

	return nil

}

// downloadChunked performs a chunked concurrent download with resume support.
// Each chunk is downloaded to a separate part file, then merged when complete.
func (d *Downloader) downloadChunked(ctx context.Context, name string, writer Writer, info *fileInfo, urls []string) error {
	existingChunks, err := d.discoverExistingChunks(name, info, writer)
	if err != nil {
		return fmt.Errorf("failed to discover existing chunks: %w", err)
	}

	// Eagerly merge all contiguous existing chunks into the writer before calculating
	// which chunks still need to be downloaded. This maximises writer coverage so that
	// the first remaining gap chunk can write directly to the writer instead of a temp file.
	err = d.tryMergeAdjacentChunks(existingChunks)
	if err != nil {
		return fmt.Errorf("failed to merge adjacent chunks: %w", err)
	}

	// Determine the writer's end byte index after the eager merge.
	writerSize, err := writer.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to get writer size: %w", err)
	}
	writerEnd := writerSize - 1 // -1 when writer is empty

	// Calculate pending chunks based on existing downloaded chunks (supports dynamic sizes)
	chunks := d.calculatehunks(name, info, existingChunks, writer, writerEnd)

	// Create a context that can be canceled on error (must be before any goroutine launch)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Progress tracking
	var downloadedBytes atomic.Int64
	var workersDownloadBytes []atomic.Int64

	if d.progressFunc != nil {
		for _, c := range existingChunks {
			if c == nil {
				continue
			}
			downloadedBytes.Add(c.end - c.start + 1)
		}
		workersDownloadBytes = make([]atomic.Int64, d.concurrency)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		go func() {
			for range ticker.C {
				totalDownloaded := downloadedBytes.Load()
				for i := range workersDownloadBytes {
					totalDownloaded += workersDownloadBytes[i].Load()
				}
				d.progressFunc(name, totalDownloaded, info.size)
			}
		}()
	}

	// Create a channel for pending chunks
	chunkCh := make(chan *chunk, d.concurrency)
	go func() {
		for _, c := range chunks {
			if c == nil || c.Existing() {
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

	reportCh := make(chan struct{}, 1)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < d.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			var chunkProgressFunc func(downloaded, total int64)
			if d.progressFunc != nil {
				chunkProgressFunc = func(downloaded, total int64) {
					workersDownloadBytes[workerID].Store(downloaded)
					select {
					case reportCh <- struct{}{}:
					default:
					}
				}
			}

			mirrorIdx := workerID % len(urls)
			for chunk := range chunkCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Try all mirrors starting from assigned one
				downloaded := false
				for attempt := 0; attempt < len(urls)*d.retryPerHost; attempt++ {
					url := urls[(mirrorIdx+attempt)%len(urls)]
					err := d.downloadChunkToFile(ctx, url, chunk, chunkProgressFunc)
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
					case errCh <- fmt.Errorf("failed to download chunk %d-%d from all mirrors", chunk.start, chunk.end):
						cancel()
					default:
					}
					return
				}

				if d.progressFunc != nil {
					downloadedBytes.Add(workersDownloadBytes[workerID].Swap(0))
				}

				chunk.SetExisting()

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

				if err := d.tryMergeAdjacentChunks(chunks); err != nil {
					select {
					case errCh <- fmt.Errorf("failed to merge adjacent chunks: %w", err):
						cancel()
					default:
					}
					return
				}
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
	finalSize, err := firstChunk.writer.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to get final size: %w", err)
	}
	if finalSize != info.size {
		return fmt.Errorf("final file size mismatch: expected %d, got %d", info.size, finalSize)
	}

	_ = d.CleanupPartFiles(name)
	return nil
}

func (d *Downloader) downloadPath(name string) string {
	return filepath.Join(d.cacheDir, name)
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

// chunkPartPath returns the path to a chunk part file.
func (d *Downloader) chunkPartPath(name string, info *fileInfo, start int64) string {
	return path.Join(
		d.downloadPath(name),
		fmt.Sprintf("offset-%d-%s", start, tempFileName(info)),
	)
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
// The writer is used for the first chunk (offset 0) instead of a part file.
func (d *Downloader) discoverExistingChunks(name string, info *fileInfo, writer Writer) ([]*chunk, error) {
	// Ensure output directory exists
	err := os.MkdirAll(d.downloadPath(name), 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Check if the writer already has data (resume from output)
	writerSize, err := writer.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	dir := d.downloadPath(name)
	entries, err := os.ReadDir(dir)
	if err != nil && writerSize == 0 {
		return nil, err
	}

	var chunks []*chunk

	// If the writer has existing data, treat it as the chunk at offset 0
	if writerSize > 0 {
		end := writerSize - 1
		if end >= info.size {
			end = info.size - 1
		}

		chunks = append(chunks, &chunk{
			start:    0,
			end:      end,
			existing: 1,
			writer:   writer,
		})
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ename := entry.Name()
		offset := parseChunkOffset(ename, info)
		if offset < 0 {
			continue
		}
		// offset 0 is handled by writer above
		if offset == 0 {
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

		chunks = append(chunks, &chunk{
			start:    offset,
			end:      end,
			existing: 1,
			writer:   newLazyWriter(filepath.Join(dir, ename), offset),
		})
	}

	// Sort by start offset
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].start < chunks[j].start
	})

	return chunks, nil
}

// calculatehunks calculates the list of chunks to download based on existing chunks.
// writerEnd is the last byte index currently covered by writer (-1 if writer is empty).
// The first gap chunk that starts at writerEnd+1 writes directly to writer instead of a temp file.
func (d *Downloader) calculatehunks(name string, info *fileInfo, existing []*chunk, writer Writer, writerEnd int64) (chunks []*chunk) {
	// Build list of covered ranges from existing chunks
	var coveredEnd int64
	for _, c := range existing {
		if c == nil {
			continue
		}
		if c.start <= coveredEnd && c.end >= coveredEnd {
			// This chunk extends our covered range
			coveredEnd = c.end + 1
			chunks = append(chunks, c)
		} else if c.start > coveredEnd {
			// There's a gap before this chunk - need to download it
			var firstChunkWriter Writer
			if coveredEnd == writerEnd+1 {
				firstChunkWriter = writer
			}
			chunks = append(chunks, d.chunksForRange(name, info, coveredEnd, c.start-1, firstChunkWriter)...)
			// This existing chunk is still valid
			coveredEnd = c.end + 1
			chunks = append(chunks, c)
		}
		// If c.end < coveredEnd, this chunk is already covered (redundant)
	}

	// Add chunks for any remaining range after the last existing chunk
	if coveredEnd < info.size {
		var firstChunkWriter Writer
		if coveredEnd == writerEnd+1 {
			firstChunkWriter = writer
		}
		chunks = append(chunks, d.chunksForRange(name, info, coveredEnd, info.size-1, firstChunkWriter)...)
	}

	return chunks
}

// chunksForRange creates chunks to cover a range [start, end].
// firstChunkWriter, if non-nil, is used as the writer for the first chunk instead of a lazyWriter.
func (d *Downloader) chunksForRange(name string, info *fileInfo, start, end int64, firstChunkWriter Writer) []*chunk {
	var chunks []*chunk
	chunkSize := d.chunkSize
	isFirst := true

	for offset := start; offset <= end; offset += chunkSize {
		chunkEnd := min(offset+chunkSize-1, end)
		var w Writer
		if isFirst && firstChunkWriter != nil {
			w = firstChunkWriter
			isFirst = false
		} else {
			w = newLazyWriter(d.chunkPartPath(name, info, offset), offset)
		}
		chunks = append(chunks, &chunk{
			start:  offset,
			end:    chunkEnd,
			writer: w,
		})
	}

	return chunks
}

// downloadChunkToFile downloads a single chunk to its writer.
// It supports resuming from a partially downloaded chunk.
func (d *Downloader) downloadChunkToFile(ctx context.Context, url string, c *chunk, progressFunc func(downloaded, total int64)) error {
	expectedSize := c.end - c.start + 1

	writerSize, err := c.writer.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// For lazyWriter the file only contains data for this chunk, so writerSize IS existingSize.
	// For a real writer the file contains all previously written chunks, so we subtract c.start.
	var existingSize int64
	_, isLazy := c.writer.(*lazyWriter)
	if isLazy {
		existingSize = writerSize
	} else {
		if writerSize > c.start {
			existingSize = writerSize - c.start
		}
	}

	if existingSize == expectedSize {
		if progressFunc != nil {
			progressFunc(expectedSize, expectedSize)
		}
		return nil
	}
	if existingSize > expectedSize {
		if _, err = c.writer.Seek(0, io.SeekStart); err != nil {
			return err
		}
		existingSize = 0
	}

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
		return fmt.Errorf("%s: unexpected status code: %d (expected 206)", url, resp.StatusCode)
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

	// For a real writer seek to the correct append position; lazyWriter handles this internally.
	if !isLazy {
		if _, err = c.writer.Seek(c.start+existingSize, io.SeekStart); err != nil {
			return err
		}
	}

	if _, err = io.Copy(c.writer, reader); err != nil {
		if isLazy {
			_ = c.writer.(*lazyWriter).Close()
		}
		return fmt.Errorf("failed to download chunk: %w", err)
	}
	if isLazy {
		if err := c.writer.(*lazyWriter).Close(); err != nil {
			return fmt.Errorf("failed to close part file: %w", err)
		}
	}

	return nil
}

// tryMergeAdjacentChunks tries to merge chunks into the first chunk (offset 0).
// If the next chunk is contiguous but still in progress, its writer is redirected to
// first.writer so future bytes are written directly to the output (no temp file).
func (d *Downloader) tryMergeAdjacentChunks(chunks []*chunk) error {
	if len(chunks) == 0 {
		return nil
	}

	first := chunks[0]
	if first.start != 0 || !first.Existing() {
		return nil
	}

	for i := 1; i < len(chunks); i++ {
		next := chunks[i]
		if next == nil {
			continue
		}
		if next.start > first.end+1 {
			return nil // gap; cannot merge
		}
		if next.writer == first.writer {
			// Already written in-place (same writer object); just extend range.
			if next.Existing() {
				first.end = next.end
				chunks[i] = nil
			}
			// If not yet existing, leave in place; we'll extend once it finishes.
			continue
		}
		lw, ok := next.writer.(*lazyWriter)
		if !ok {
			return fmt.Errorf("expected chunk.writer to be a *lazyWriter for non-zero offset part file")
		}
		if !next.Existing() {
			// Contiguous in-progress chunk: redirect its writer to first.writer so all
			// subsequent bytes flow directly to the output without a temp file.
			if !lw.IsRedirected() {
				if err := lw.Redirect(first.writer, d.mergeChunkFileIntoWriter); err != nil {
					return fmt.Errorf("failed to redirect chunk writer: %w", err)
				}
			}
			// Don't advance: we need to wait for this chunk to finish before
			// looking at the chunk after it.
			return nil
		}
		if lw.IsRedirected() {
			// Chunk completed via redirect: all bytes are already in first.writer.
			first.end = next.end
			chunks[i] = nil
			continue
		}
		// Normal merge of a fully-downloaded lazy chunk.
		if err := lw.Close(); err != nil {
			return fmt.Errorf("failed to close part file: %w", err)
		}
		if err := d.mergeChunkFileIntoWriter(first.writer, lw.Path(), next.start); err != nil {
			return fmt.Errorf("failed to merge chunk file: %w", err)
		}
		first.end = next.end
		chunks[i] = nil
	}

	return nil
}

// mergeChunkFileIntoWriter appends the content of srcFile to writer.
func (d *Downloader) mergeChunkFileIntoWriter(writer Writer, srcFile string, offset int64) error {
	src, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer src.Close()

	dstSize, err := writer.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if dstSize < offset {
		return fmt.Errorf("writer size %d is less than expected offset %d", dstSize, offset)
	}

	if dstSize > offset {
		_, err = src.Seek(dstSize-offset, io.SeekStart)
		if err != nil {
			return err
		}
	}

	_, err = io.Copy(writer, src)
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
func (d *Downloader) CleanupPartFiles(name string) error {
	return os.RemoveAll(d.downloadPath(name))
}

// progressReader wraps an io.Reader to report progress.
type progressReader struct {
	ctx          context.Context
	reader       io.Reader
	total        int64
	read         int64
	progressFunc func(downloaded, total int64)
}

func (pr *progressReader) Read(p []byte) (int, error) {
	if err := pr.ctx.Err(); err != nil {
		return 0, err
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
