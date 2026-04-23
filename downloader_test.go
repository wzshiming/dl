package dl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// newTestWriter creates a temp file that implements Writer, cleaned up after the test.
func newTestWriter(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "dl-writer-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// readAll seeks to start and reads all content from a Writer-compatible file.
func readAll(t *testing.T, f *os.File) []byte {
	t.Helper()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// newFileServer creates a test HTTP server backed by static data.
// If supportsRange is true the server advertises and handles range requests.
func newFileServer(t *testing.T, data []byte, etag string, supportsRange bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if supportsRange {
				w.Header().Set("Accept-Ranges", "bytes")
			}
			if etag != "" {
				w.Header().Set("ETag", etag)
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			rangeHdr := r.Header.Get("Range")
			if rangeHdr == "" || !supportsRange {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
				w.WriteHeader(http.StatusOK)
				w.Write(data)
				return
			}
			var start, end int64
			fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
			if end >= int64(len(data)) {
				end = int64(len(data)) - 1
			}
			seg := data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(seg)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(seg)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// generateData returns a deterministic byte slice of the given length.
func generateData(size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i % 251)
	}
	return buf
}

// --- pure function tests ---

func TestNormalizeEtag(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{`"abc123"`, "abc123"},
		{`W/"abc123"`, "abc123"},
		{`abc123`, "abc123"},
		{`W/"weak"`, "weak"},
	}
	for _, tc := range cases {
		got := normalizeEtag(tc.input)
		if got != tc.want {
			t.Errorf("normalizeEtag(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestTempFileName(t *testing.T) {
	t.Run("with etag", func(t *testing.T) {
		info := &fileInfo{etag: `"abc123"`, size: 100}
		got := tempFileName(info)
		if got != "etag-abc123" {
			t.Errorf("got %q, want %q", got, "etag-abc123")
		}
	})
	t.Run("with size only", func(t *testing.T) {
		info := &fileInfo{size: 12345}
		got := tempFileName(info)
		if got != "size-12345" {
			t.Errorf("got %q, want %q", got, "size-12345")
		}
	})
	t.Run("unknown", func(t *testing.T) {
		info := &fileInfo{}
		if got := tempFileName(info); got != "unknown" {
			t.Errorf("got %q, want %q", got, "unknown")
		}
	})
}

func TestParseChunkOffset(t *testing.T) {
	info := &fileInfo{size: 1000}

	cases := []struct {
		name string
		want int64
	}{
		{"offset-0-size-1000", 0},
		{"offset-512-size-1000", 512},
		{"offset-999-size-1000", 999},
		{"notoffset-0-size-1000", -1},
		{"offset-abc-size-1000", -1},
		{"offset-0-size-999", -1}, // wrong suffix
		{"something-else", -1},
	}
	for _, tc := range cases {
		got := parseChunkOffset(tc.name, info)
		if got != tc.want {
			t.Errorf("parseChunkOffset(%q) = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// --- Download: error cases ---

func TestDownload_NoMirrors(t *testing.T) {
	d := NewDownloader(WithCacheDir(t.TempDir()))
	w := newTestWriter(t)
	err := d.Download(context.Background(), "test", w)
	if err != ErrNoMirrors {
		t.Fatalf("expected ErrNoMirrors, got %v", err)
	}
}

func TestDownload_AlreadyComplete(t *testing.T) {
	data := generateData(64)
	srv := newFileServer(t, data, "", false)
	d := NewDownloader(WithCacheDir(t.TempDir()))

	w := newTestWriter(t)
	// Pre-fill writer with the full content.
	w.Write(data)

	err := d.Download(context.Background(), "test", w, srv.URL)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	// Content should be unchanged.
	got := readAll(t, w)
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch after already-complete check")
	}
}

// --- Download: direct (no range support) ---

func TestDownload_Direct(t *testing.T) {
	data := generateData(1024)
	srv := newFileServer(t, data, "", false)

	d := NewDownloader(WithCacheDir(t.TempDir()))
	w := newTestWriter(t)

	if err := d.Download(context.Background(), "test", w, srv.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := readAll(t, w)
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestDownload_DirectWithProgress(t *testing.T) {
	data := generateData(512)
	srv := newFileServer(t, data, "", false)

	var calls atomic.Int64
	d := NewDownloader(
		WithCacheDir(t.TempDir()),
		WithProgressFunc(func(name string, downloaded, total int64) {
			calls.Add(1)
		}),
	)
	w := newTestWriter(t)
	if err := d.Download(context.Background(), "test", w, srv.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls.Load() == 0 {
		t.Fatal("progress callback was never called")
	}
}

func TestDownload_DirectMirrorFallback(t *testing.T) {
	data := generateData(256)
	good := newFileServer(t, data, "", false)

	// First URL is a server that always 500s.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "error", http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)

	d := NewDownloader(WithCacheDir(t.TempDir()))
	w := newTestWriter(t)
	if err := d.Download(context.Background(), "test", w, bad.URL, good.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := readAll(t, w)
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch after mirror fallback")
	}
}

// --- Download: chunked ---

func TestDownload_Chunked(t *testing.T) {
	data := generateData(1024)
	srv := newFileServer(t, data, `"etag-abc"`, true)

	d := NewDownloader(
		WithCacheDir(t.TempDir()),
		WithChunkSize(256),
		WithConcurrency(2),
	)
	w := newTestWriter(t)
	if err := d.Download(context.Background(), "test", w, srv.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := readAll(t, w)
	if !bytes.Equal(got, data) {
		t.Fatalf("chunked content mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestDownload_ChunkedWithProgress(t *testing.T) {
	data := generateData(1024)
	srv := newFileServer(t, data, "", true)

	var calls atomic.Int64
	d := NewDownloader(
		WithCacheDir(t.TempDir()),
		WithChunkSize(256),
		WithConcurrency(2),
		WithProgressFunc(func(name string, downloaded, total int64) {
			calls.Add(1)
		}),
	)
	w := newTestWriter(t)
	if err := d.Download(context.Background(), "test", w, srv.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := readAll(t, w)
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch with progress: got %d bytes, want %d", len(got), len(data))
	}
	// Progress callback may or may not fire for fast downloads (ticker-based); just verify it doesn't panic.
	_ = calls.Load()
}

func TestDownload_ChunkedResumeFromWriter(t *testing.T) {
	data := generateData(1024)
	srv := newFileServer(t, data, `"etag-resume"`, true)
	cacheDir := t.TempDir()

	// First pass: download first 256 bytes manually into writer to simulate partial progress.
	w := newTestWriter(t)
	w.Write(data[:256])

	d := NewDownloader(
		WithCacheDir(cacheDir),
		WithChunkSize(256),
		WithConcurrency(2),
		WithResume(true),
	)
	if err := d.Download(context.Background(), "resume", w, srv.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := readAll(t, w)
	if !bytes.Equal(got, data) {
		t.Fatalf("resume content mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

// TestDownload_ChunkedResumeWriterPlusExistingPart verifies that when the writer already
// has data AND a later part file also exists, the gap chunk between them writes directly
// to the writer (not to a temp file), and the final result is correct.
func TestDownload_ChunkedResumeWriterPlusExistingPart(t *testing.T) {
	data := generateData(1024)
	const etag = `"etag-part"`
	srv := newFileServer(t, data, etag, true)
	cacheDir := t.TempDir()

	// Writer has [0, 255].
	w := newTestWriter(t)
	w.Write(data[:256])

	// Create a part file for chunk [512, 767] to simulate a partially-complete download.
	// This leaves gap [256, 511] which should be fetched directly into the writer.
	partDir := filepath.Join(cacheDir, "dl", "resume-part")
	if err := os.MkdirAll(partDir, 0755); err != nil {
		t.Fatal(err)
	}
	partFile := filepath.Join(partDir, "offset-512-etag-etag-part")
	if err := os.WriteFile(partFile, data[512:768], 0644); err != nil {
		t.Fatal(err)
	}

	d := NewDownloader(
		WithCacheDir(cacheDir),
		WithChunkSize(256),
		WithConcurrency(2),
		WithResume(true),
	)
	if err := d.Download(context.Background(), "resume-part", w, srv.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := readAll(t, w)
	if !bytes.Equal(got, data) {
		t.Fatalf("resume+part content mismatch: got %d bytes, want %d", len(got), len(data))
	}
	// The gap part file (offset-256) should not exist because [256,511] was written directly to w.
	gapPartFile := filepath.Join(partDir, "offset-256-etag-etag-part")
	if _, err := os.Stat(gapPartFile); err == nil {
		t.Fatal("expected no part file for offset-256 (gap should have been written directly to writer)")
	}
}

func TestDownload_ChunkedResumeDisabled(t *testing.T) {
	data := generateData(1024)
	srv := newFileServer(t, data, "", true)

	w := newTestWriter(t)
	w.Write(data[:256]) // partial content

	d := NewDownloader(
		WithCacheDir(t.TempDir()),
		WithChunkSize(256),
		WithResume(false),
	)
	err := d.Download(context.Background(), "test", w, srv.URL)
	if err == nil {
		t.Fatal("expected error when resume is disabled but output is partial, got nil")
	}
}

func TestDownload_OutputLargerThanExpected(t *testing.T) {
	data := generateData(512)
	srv := newFileServer(t, data, "", false)

	w := newTestWriter(t)
	w.Write(generateData(1024)) // larger than the remote file

	d := NewDownloader(WithCacheDir(t.TempDir()))
	err := d.Download(context.Background(), "test", w, srv.URL)
	if err == nil {
		t.Fatal("expected error for oversized output, got nil")
	}
}

func TestDownload_Canceled(t *testing.T) {
	data := generateData(1024)
	// Server that blocks until context is done.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			// Write first byte then block.
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	d := NewDownloader(WithCacheDir(t.TempDir()))
	w := newTestWriter(t)
	err := d.Download(ctx, "test", w, srv.URL)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// --- merge helpers ---

func TestMergeChunkFileIntoWriter(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.WriteFile(src, []byte(" world"), 0644); err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(t)
	w.Write([]byte("hello"))

	d := NewDownloader()
	if err := d.mergeChunkFileIntoWriter(w, src, 5); err != nil {
		t.Fatalf("mergeChunkFileIntoWriter: %v", err)
	}

	got := readAll(t, w)
	if string(got) != "hello world" {
		t.Fatalf("got %q, want %q", string(got), "hello world")
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("src file should have been removed")
	}
}

func TestMergeChunkFileIntoWriter_Resume(t *testing.T) {
	// writer already contains more bytes than offset (partial overlap).
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.WriteFile(src, []byte(" world"), 0644); err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(t)
	w.Write([]byte("hello wo")) // 8 bytes, offset=5 means 3 bytes overlap

	d := NewDownloader()
	if err := d.mergeChunkFileIntoWriter(w, src, 5); err != nil {
		t.Fatalf("mergeChunkFileIntoWriter: %v", err)
	}

	got := readAll(t, w)
	if string(got) != "hello world" {
		t.Fatalf("got %q, want %q", string(got), "hello world")
	}
}

// --- discoverExistingChunks ---

func TestDiscoverExistingChunks_EmptyDir(t *testing.T) {
	d := NewDownloader(WithCacheDir(t.TempDir()))
	info := &fileInfo{size: 1000}
	w := newTestWriter(t)
	chunks, err := d.discoverExistingChunks("name", info, w)
	if err != nil {
		t.Fatalf("discoverExistingChunks: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks, got %d", len(chunks))
	}
}

func TestDiscoverExistingChunks_WriterHasData(t *testing.T) {
	d := NewDownloader(WithCacheDir(t.TempDir()))
	info := &fileInfo{size: 1000}
	w := newTestWriter(t)
	w.Write(make([]byte, 300))

	chunks, err := d.discoverExistingChunks("name", info, w)
	if err != nil {
		t.Fatalf("discoverExistingChunks: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	c := chunks[0]
	if c.start != 0 || c.end != 299 {
		t.Fatalf("unexpected chunk range [%d, %d]", c.start, c.end)
	}
	if c.writer == nil {
		t.Fatal("expected chunk.writer to be set")
	}
}

func TestDiscoverExistingChunks_PartFiles(t *testing.T) {
	cacheDir := t.TempDir()
	d := NewDownloader(WithCacheDir(cacheDir))
	info := &fileInfo{size: 1000}

	// Create a part file at offset 512.
	partDir := filepath.Join(cacheDir, "dl-cache", "name")
	if err := os.MkdirAll(partDir, 0755); err != nil {
		t.Fatal(err)
	}
	partName := fmt.Sprintf("offset-512-size-1000")
	if err := os.WriteFile(filepath.Join(partDir, partName), make([]byte, 200), 0644); err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(t)
	chunks, err := d.discoverExistingChunks("name", info, w)
	if err != nil {
		t.Fatalf("discoverExistingChunks: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	c := chunks[0]
	if c.start != 512 || c.end != 711 {
		t.Fatalf("unexpected chunk range [%d, %d]", c.start, c.end)
	}
	if _, ok := c.writer.(*lazyWriter); !ok {
		t.Fatal("expected chunk.writer to be a *lazyWriter for non-zero offset part file")
	}
}

// --- tryMergeAdjacentChunks ---

func TestTryMergeAdjacentChunks_IntoWriter(t *testing.T) {
	dir := t.TempDir()
	w := newTestWriter(t)
	w.Write([]byte("hello"))

	// Create a part file for the next chunk.
	srcPath := filepath.Join(dir, "next")
	if err := os.WriteFile(srcPath, []byte(" world"), 0644); err != nil {
		t.Fatal(err)
	}

	first := &chunk{start: 0, end: 4, writer: w}
	first.SetExisting()
	second := &chunk{start: 5, end: 10, writer: newLazyWriter(srcPath, 5)}
	second.SetExisting()

	chunks := []*chunk{first, second}
	d := NewDownloader()
	d.tryMergeAdjacentChunks(chunks)

	if first.end != 10 {
		t.Fatalf("expected first.end=10, got %d", first.end)
	}
	if chunks[1] != nil {
		t.Fatal("expected second chunk to be nil after merge")
	}
	got := readAll(t, w)
	if string(got) != "hello world" {
		t.Fatalf("merged content = %q, want %q", string(got), "hello world")
	}
}

func TestTryMergeAdjacentChunks_GapPreventssMerge(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "next")
	if err := os.WriteFile(srcPath, []byte(" world"), 0644); err != nil {
		t.Fatal(err)
	}

	w := newTestWriter(t)
	w.Write([]byte("hello"))

	first := &chunk{start: 0, end: 4, writer: w}
	first.SetExisting()
	second := &chunk{start: 10, end: 15, writer: newLazyWriter(srcPath, 10)} // gap at 5-9
	second.SetExisting()

	chunks := []*chunk{first, second}
	d := NewDownloader()
	d.tryMergeAdjacentChunks(chunks)

	// Should NOT merge due to gap.
	if first.end != 4 {
		t.Fatalf("expected first.end=4, got %d", first.end)
	}
	if chunks[1] == nil {
		t.Fatal("second chunk should not be consumed when there is a gap")
	}
}

// TestTryMergeAdjacentChunks_RedirectInProgress verifies that when the next chunk is
// contiguous but not yet fully downloaded, tryMergeAdjacentChunks redirects its
// lazyWriter to first.writer so ongoing and future bytes land directly in the output.
func TestTryMergeAdjacentChunks_RedirectInProgress(t *testing.T) {
	dir := t.TempDir()
	w := newTestWriter(t)
	w.Write([]byte("hello")) // first chunk [0,4] complete

	// Second chunk [5,10] has only written " wo" so far (in progress).
	srcPath := filepath.Join(dir, "next")
	if err := os.WriteFile(srcPath, []byte(" wo"), 0644); err != nil {
		t.Fatal(err)
	}

	first := &chunk{start: 0, end: 4, writer: w}
	first.SetExisting()
	second := &chunk{start: 5, end: 10, writer: newLazyWriter(srcPath, 5)}
	// second is NOT yet existing (still downloading)

	chunks := []*chunk{first, second}
	d := NewDownloader()
	if err := d.tryMergeAdjacentChunks(chunks); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The lazyWriter should be redirected.
	lw := second.writer.(*lazyWriter)
	if !lw.IsRedirected() {
		t.Fatal("expected second chunk's lazyWriter to be redirected to first.writer")
	}
	// The already-downloaded bytes should have been merged into w.
	got := readAll(t, w)
	if string(got) != "hello wo" {
		t.Fatalf("after redirect merge got %q, want %q", string(got), "hello wo")
	}
	// The temp file should no longer exist.
	if _, err := os.Stat(srcPath); err == nil {
		t.Fatal("expected temp part file to be removed after redirect")
	}
	// second chunk still not nil (still in progress, not yet existing).
	if chunks[1] == nil {
		t.Fatal("second chunk should not be nil while still in progress")
	}

	// Simulate the worker writing remaining bytes " rld" directly to the redirected writer.
	if _, err := second.writer.Write([]byte("rld")); err != nil {
		t.Fatalf("write after redirect: %v", err)
	}
	// Mark as existing and run merge again.
	second.SetExisting()
	if err := d.tryMergeAdjacentChunks(chunks); err != nil {
		t.Fatalf("unexpected error on second merge: %v", err)
	}
	if first.end != 10 {
		t.Fatalf("expected first.end=10 after second merge, got %d", first.end)
	}
	if chunks[1] != nil {
		t.Fatal("expected second chunk to be nil after completing")
	}
	got = readAll(t, w)
	if string(got) != "hello world" {
		t.Fatalf("final content = %q, want %q", string(got), "hello world")
	}
}
