// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux
// +build linux

package nbio

/*
#include <sys/epoll.h>

typedef struct epoll_event EpollEvent;
typedef epoll_data_t EpollData;
*/
import "C"

import (
	"errors"
	"io"
	"net"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"github.com/lesismal/nbio/logging"
)

const (
	// EPOLLLT .
	EPOLLLT = 0

	// EPOLLET .
	EPOLLET = 0x80000000
)

const (
	epollEventsRead      = syscall.EPOLLPRI | syscall.EPOLLIN
	epollEventsWrite     = syscall.EPOLLOUT
	epollEventsReadWrite = syscall.EPOLLPRI | syscall.EPOLLIN | syscall.EPOLLOUT
	epollEventsError     = syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP

	epollEventsReadET      = syscall.EPOLLPRI | syscall.EPOLLIN | EPOLLET
	epollEventsReadWriteET = syscall.EPOLLPRI | syscall.EPOLLIN | syscall.EPOLLOUT | EPOLLET
)

type poller struct {
	g *Gopher

	epfd  int
	evtfd int

	index int

	shutdown bool

	listener   net.Listener
	isListener bool

	ReadBuffer []byte

	pollType string
}

func (p *poller) addConn(c *Conn) {
	c.g = p.g
	p.g.onOpen(c)
	// fd := c.fd
	// p.g.connsUnix[fd] = c
	err := p.addRead(c)
	if err != nil {
		// p.g.connsUnix[fd] = nil
		c.closeWithError(err)
		logging.Error("[%v] add read event failed: %v", c.fd, err)
		return
	}
}

func (p *poller) getConn(fd int) *Conn {
	return p.g.connsUnix[fd]
}

func (p *poller) deleteConn(c *Conn) {
	if c == nil {
		return
	}
	fd := c.fd
	if c == p.g.connsUnix[fd] {
		p.g.connsUnix[fd] = nil
		p.deleteEvent(fd)
	}
	p.g.onClose(c, c.closeErr)
}

func (p *poller) start() {
	defer p.g.Done()

	logging.Debug("Poller[%v_%v_%v] start", p.g.Name, p.pollType, p.index)
	defer logging.Debug("Poller[%v_%v_%v] stopped", p.g.Name, p.pollType, p.index)

	if p.isListener {
		p.acceptorLoop()
	} else {
		defer func() {
			syscall.Close(p.epfd)
			syscall.Close(p.evtfd)
		}()
		p.readWriteLoop()
	}
}

func (p *poller) acceptorLoop() {
	if p.g.lockListener {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	p.shutdown = false
	for !p.shutdown {
		conn, err := p.listener.Accept()
		if err == nil {
			var c *Conn
			c, err = NBConn(conn)
			if err != nil {
				conn.Close()
				continue
			}
			o := p.g.pollers[c.fd%len(p.g.pollers)]
			o.addConn(c)
		} else {
			var ne net.Error
			if ok := errors.As(err, &ne); ok && ne.Temporary() {
				logging.Error("Poller[%v_%v_%v] Accept failed: temporary error, retrying...", p.g.Name, p.pollType, p.index)
				time.Sleep(time.Second / 20)
			} else {
				logging.Error("Poller[%v_%v_%v] Accept failed: %v, exit...", p.g.Name, p.pollType, p.index, err)
				break
			}
		}
	}
}

func (p *poller) readWriteLoop() {
	if p.g.lockPoller {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	msec := -1
	events := make([]C.EpollEvent, 1024)

	if p.g.onRead == nil && p.g.epollMod == syscall.EPOLLET {
		p.g.maxReadTimesPerEventLoop = 1<<31 - 1
	}

	p.shutdown = false

	for !p.shutdown {
		n, err := epollWait(p.epfd, events, msec)
		if err != nil && !errors.Is(err, syscall.EINTR) {
			return
		}

		if n <= 0 {
			msec = -1
			// runtime.Gosched()
			continue
		}
		msec = 20

		for _, ev := range events[:n] {
			c := *((**Conn)(unsafe.Pointer(&ev.data)))
			switch c.fd {
			case p.evtfd:
			default:
				if c != nil {
					if ev.events&epollEventsError != 0 {
						c.closeWithError(io.EOF)
						continue
					}

					if ev.events&epollEventsWrite != 0 {
						c.flush()
					}

					if ev.events&epollEventsRead != 0 {
						if p.g.onRead == nil {
							for i := 0; i < p.g.maxReadTimesPerEventLoop; i++ {
								buffer := p.g.borrow(c)
								n, err := c.Read(buffer)
								if n > 0 {
									p.g.onData(c, buffer[:n])
								}
								p.g.payback(c, buffer)
								if errors.Is(err, syscall.EINTR) {
									continue
								}
								if errors.Is(err, syscall.EAGAIN) {
									break
								}
								if err != nil || n == 0 {
									c.closeWithError(err)
								}
								if n < len(buffer) {
									break
								}
							}
						} else {
							p.g.onRead(c)
						}
					}
				} else {
					syscall.Close(c.fd)
					p.deleteEvent(c.fd)
				}
			}
		}
	}
}

func (p *poller) stop() {
	logging.Debug("Poller[%v_%v_%v] stop...", p.g.Name, p.pollType, p.index)
	p.shutdown = true
	if p.listener != nil {
		p.listener.Close()
	} else {
		n := uint64(1)
		syscall.Write(p.evtfd, (*(*[8]byte)(unsafe.Pointer(&n)))[:])
	}
}

func (p *poller) addRead(c *Conn) error {
	ptr := *((*[8]byte)(unsafe.Pointer(&c)))
	switch p.g.epollMod {
	case EPOLLET:
		_, _, err := syscall.RawSyscall6(syscall.SYS_EPOLL_CTL, uintptr(p.epfd), uintptr(syscall.EPOLL_CTL_ADD), uintptr(c.fd),
			uintptr(unsafe.Pointer(&C.EpollEvent{data: C.EpollData{ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6], ptr[7]}, events: epollEventsReadET})),
			0, 0)
		if err == 0 {
			return nil
		}
		return err
	default:
		_, _, err := syscall.RawSyscall6(syscall.SYS_EPOLL_CTL, uintptr(p.epfd), uintptr(syscall.EPOLL_CTL_ADD), uintptr(c.fd),
			uintptr(unsafe.Pointer(&C.EpollEvent{data: C.EpollData{ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6], ptr[7]}, events: epollEventsRead})),
			0, 0)
		if err == 0 {
			return nil
		}
		return err
	}
}

// func (p *poller) addWrite(fd int) error {
// 	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{Fd: int32(fd), Events: syscall.EPOLLOUT})
// }

func (p *poller) modWrite(c *Conn) error {
	ptr := *((*[8]byte)(unsafe.Pointer(&c)))
	switch p.g.epollMod {
	case EPOLLET:
		_, _, err := syscall.RawSyscall6(syscall.SYS_EPOLL_CTL, uintptr(p.epfd), uintptr(syscall.EPOLL_CTL_ADD), uintptr(c.fd),
			uintptr(unsafe.Pointer(&C.EpollEvent{data: C.EpollData{ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6], ptr[7]}, events: epollEventsReadWriteET})),
			0, 0)
		if err == 0 {
			return nil
		}
		return err
	default:
		_, _, err := syscall.RawSyscall6(syscall.SYS_EPOLL_CTL, uintptr(p.epfd), uintptr(syscall.EPOLL_CTL_MOD), uintptr(c.fd),
			uintptr(unsafe.Pointer(&C.EpollEvent{data: C.EpollData{ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6], ptr[7]}, events: epollEventsReadWrite})),
			0, 0)
		if err == 0 {
			return nil
		}
		return err
	}
}

func (p *poller) deleteEvent(fd int) error {
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_DEL, fd, &syscall.EpollEvent{Fd: int32(fd)})
}

func newPoller(g *Gopher, isListener bool, index int) (*poller, error) {
	if isListener {
		if len(g.addrs) == 0 {
			panic("invalid listener num")
		}

		addr := g.addrs[index%len(g.listeners)]
		ln, err := net.Listen(g.network, addr)
		if err != nil {
			return nil, err
		}

		p := &poller{
			g:          g,
			index:      index,
			listener:   ln,
			isListener: isListener,
			pollType:   "LISTENER",
		}

		return p, nil
	}

	fd, err := syscall.EpollCreate1(0)
	if err != nil {
		return nil, err
	}

	r0, _, e0 := syscall.Syscall(syscall.SYS_EVENTFD2, 0, syscall.O_NONBLOCK, 0)
	if e0 != 0 {
		syscall.Close(fd)
		return nil, err
	}

	// err = syscall.EpollCtl(fd, syscall.EPOLL_CTL_ADD, int(r0),
	// 	&syscall.EpollEvent{Fd: int32(r0),
	// 		Events: syscall.EPOLLIN,
	// 	},
	// )
	c := &Conn{fd: int(r0)}
	ptr := *((*[8]byte)(unsafe.Pointer(&c)))
	_, _, e1 := syscall.RawSyscall6(syscall.SYS_EPOLL_CTL, uintptr(fd), uintptr(syscall.EPOLL_CTL_ADD), uintptr(c.fd),
		uintptr(unsafe.Pointer(&C.EpollEvent{data: C.EpollData{ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6], ptr[7]}, events: syscall.EPOLLIN})),
		0, 0)
	if e1 == 0 {
		err = nil
	} else {
		err = e1
	}
	if err != nil {
		syscall.Close(fd)
		syscall.Close(int(r0))
		return nil, err
	}

	p := &poller{
		g:          g,
		epfd:       fd,
		evtfd:      int(r0),
		index:      index,
		isListener: isListener,
		pollType:   "POLLER",
	}

	return p, nil
}

var _zero uintptr

func epollWait(epfd int, events []C.EpollEvent, msec int) (n int, err error) {
	var _p0 unsafe.Pointer
	if len(events) > 0 {
		_p0 = unsafe.Pointer(&events[0])
	} else {
		_p0 = unsafe.Pointer(&_zero)
	}
	r0, _, e1 := syscall.Syscall6(syscall.SYS_EPOLL_WAIT, uintptr(epfd), uintptr(_p0), uintptr(len(events)), uintptr(msec), 0, 0)
	n = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}
