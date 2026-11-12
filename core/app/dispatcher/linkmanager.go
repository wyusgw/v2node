package dispatcher

import (
	"io"
	sync "sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

type ManagedWriter struct {
	writer  buf.Writer
	manager *LinkManager
	// Close/Interrupt can be no-ops on DispatchLink's raw client conns, so CloseAll sets this
	// and the next read/write fails, making the protocol handler tear the connection down.
	killed atomic.Bool
	// Killing only the upload side would leave target data flowing to the client.
	downlink []interface{}
	ip       string
	// Splice runs on the raw sockets and never touches the pipes, so only closing conn ends it.
	conn net.Conn
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if w.killed.Load() {
		buf.ReleaseMulti(mb)
		return io.ErrClosedPipe
	}
	return w.writer.WriteMultiBuffer(mb)
}

type ManagedReader struct {
	reader buf.TimeoutReader
	writer *ManagedWriter
}

func NewManagedReader(reader buf.Reader, writer *ManagedWriter) *ManagedReader {
	return &ManagedReader{
		reader: &buf.TimeoutWrapperReader{Reader: reader},
		writer: writer,
	}
}

func (r *ManagedReader) check(mb buf.MultiBuffer, err error) (buf.MultiBuffer, error) {
	if r.writer.killed.Load() {
		buf.ReleaseMulti(mb)
		return nil, io.ErrClosedPipe
	}
	return mb, err
}

func (r *ManagedReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	return r.check(r.reader.ReadMultiBuffer())
}

func (r *ManagedReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	return r.check(r.reader.ReadMultiBufferTimeout(timeout))
}

func (r *ManagedReader) Interrupt() {
	common.Interrupt(r.reader)
}

func (w *ManagedWriter) Close() error {
	w.manager.RemoveWriter(w)
	return common.Close(w.writer)
}

// Without it common.Interrupt falls back to Close, which drains buffered data first.
func (w *ManagedWriter) Interrupt() {
	w.manager.RemoveWriter(w)
	common.Interrupt(w.writer)
}

type LinkManager struct {
	links  map[*ManagedWriter]buf.Reader
	mu     sync.RWMutex
	closed bool
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	closed := m.closed
	if !closed {
		m.links[writer] = reader
	}
	m.mu.Unlock()
	if closed {
		killLink(writer, reader)
	}
}

func (m *LinkManager) RemoveWriter(writer *ManagedWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		delete(m.links, writer)
	}
}

func (m *LinkManager) CloseAll() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true

	links := m.links
	m.links = make(map[*ManagedWriter]buf.Reader)
	m.mu.Unlock()

	for w, r := range links {
		killLink(w, r)
	}
}

func (m *LinkManager) IPs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	seen := make(map[string]struct{}, len(m.links))
	ips := make([]string, 0, len(m.links))
	for w := range m.links {
		if _, ok := seen[w.ip]; !ok && w.ip != "" {
			seen[w.ip] = struct{}{}
			ips = append(ips, w.ip)
		}
	}
	return ips
}

func (m *LinkManager) CloseIP(ip string) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	links := make(map[*ManagedWriter]buf.Reader)
	for w, r := range m.links {
		if w.ip == ip {
			links[w] = r
			delete(m.links, w)
		}
	}
	m.mu.Unlock()

	for w, r := range links {
		killLink(w, r)
	}
}

func killLink(w *ManagedWriter, r buf.Reader) {
	w.killed.Store(true)
	for _, p := range w.downlink {
		common.Interrupt(p)
		common.Close(p)
	}
	common.Close(w.writer)
	common.Interrupt(r)
	if w.conn != nil {
		// TLS close can wait on a stalled client to send its alert
		go w.conn.Close()
	}
}
