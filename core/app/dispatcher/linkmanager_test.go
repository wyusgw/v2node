package dispatcher

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

type fakeConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *fakeConn) Close() error {
	c.closed.Store(true)
	return nil
}

type nopWriter struct{}

func (nopWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	buf.ReleaseMulti(mb)
	return nil
}

func newManager() *LinkManager {
	return &LinkManager{links: make(map[*ManagedWriter]buf.Reader)}
}

func waitClosed(t *testing.T, c *fakeConn) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !c.closed.Load() {
		if time.Now().After(deadline) {
			t.Fatal("client connection was not closed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCloseAllClosesClientConn(t *testing.T) {
	m := newManager()
	conn := &fakeConn{}
	w := &ManagedWriter{writer: nopWriter{}, manager: m, conn: conn}
	m.AddLink(w, nil)
	m.CloseAll()
	waitClosed(t, conn)
	if !w.killed.Load() {
		t.Fatal("link was not killed")
	}
}

func TestCloseIPOnlyClosesThatDevice(t *testing.T) {
	m := newManager()
	kicked := &fakeConn{}
	other := &fakeConn{}
	m.AddLink(&ManagedWriter{writer: nopWriter{}, manager: m, conn: kicked, ip: "1.1.1.1"}, nil)
	m.AddLink(&ManagedWriter{writer: nopWriter{}, manager: m, conn: other, ip: "2.2.2.2"}, nil)
	m.CloseIP("1.1.1.1")
	waitClosed(t, kicked)
	time.Sleep(20 * time.Millisecond)
	if other.closed.Load() {
		t.Fatal("other device was closed")
	}
}

func TestAddLinkAfterCloseAllKillsLink(t *testing.T) {
	m := newManager()
	m.CloseAll()
	conn := &fakeConn{}
	w := &ManagedWriter{writer: nopWriter{}, manager: m, conn: conn}
	m.AddLink(w, nil)
	waitClosed(t, conn)
	if !w.killed.Load() {
		t.Fatal("late link was not killed")
	}
}

func TestLinkWithoutConn(t *testing.T) {
	m := newManager()
	w := &ManagedWriter{writer: nopWriter{}, manager: m}
	m.AddLink(w, nil)
	m.CloseAll()
	if !w.killed.Load() {
		t.Fatal("link was not killed")
	}
}
