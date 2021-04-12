// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

import (
	"errors"
	"net"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/lesismal/nbio/loging"
)

// Start init and start pollers
func (g *Gopher) Start() error {
	var err error

	g.lfds = []int{}

	for _, addr := range g.addrs {
		fd, err := listen(g.network, addr, g.maxLoad)
		if err != nil {
			return err
		}

		g.lfds = append(g.lfds, fd)
	}

	for i := 0; i < g.listenerNum; i++ {
		g.listeners[i], err = newPoller(g, true, int(i))
		if err != nil {
			for j := 0; j < int(i); j++ {
				syscall.Close(g.lfds[j])
				g.listeners[j].stop()
			}
			return err
		}
	}

	for i := 0; i < g.pollerNum; i++ {
		g.pollers[i], err = newPoller(g, false, int(i))
		if err != nil {
			for j := 0; j < int(len(g.lfds)); j++ {
				syscall.Close(g.lfds[j])
				g.listeners[j].stop()
			}

			for j := 0; j < int(i); j++ {
				g.pollers[j].stop()
			}
			return err
		}
	}

	for i := 0; i < g.pollerNum; i++ {
		g.pollers[i].ReadBuffer = make([]byte, g.readBufferSize)
		g.Add(1)
		go g.pollers[i].start()
	}
	for _, l := range g.listeners {
		g.Add(1)
		go l.start()
	}

	g.Add(1)
	go g.timerLoop()

	if len(g.addrs) == 0 {
		loging.Info("Gopher[%v] start", g.Name)
	} else {
		loging.Info("Gopher[%v] start listen on: [\"%v\"]", g.Name, strings.Join(g.addrs, `", "`))
	}
	return nil
}

// Conn converts net.Conn to *Conn
func (g *Gopher) Conn(conn net.Conn) (*Conn, error) {
	if conn == nil {
		return nil, errors.New("invalid conn: nil")
	}
	c, ok := conn.(*Conn)
	if !ok {
		var err error
		c, err = dupStdConn(conn)
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

// NewGopher is a factory impl
func NewGopher(conf Config) *Gopher {
	cpuNum := runtime.NumCPU()
	if conf.Name == "" {
		conf.Name = "NB"
	}
	if conf.MaxLoad <= 0 {
		conf.MaxLoad = DefaultMaxLoad
	}
	if conf.NPoller <= 0 {
		conf.NPoller = cpuNum
	}
	if conf.ReadBufferSize <= 0 {
		conf.ReadBufferSize = DefaultReadBufferSize
	}
	if conf.MinConnCacheSize == 0 {
		conf.MinConnCacheSize = DefaultMinConnCacheSize
	}

	var epollMod int = 0
	if conf.EPOLLMOD == EPOLLET {
		epollMod = syscall.EPOLLET
	}

	conf.NListener = 1

	g := &Gopher{
		Name:               conf.Name,
		network:            conf.Network,
		addrs:              conf.Addrs,
		maxLoad:            int64(conf.MaxLoad),
		epollMod:           epollMod,
		listenerNum:        conf.NListener,
		pollerNum:          conf.NPoller,
		readBufferSize:     conf.ReadBufferSize,
		maxWriteBufferSize: conf.MaxWriteBufferSize,
		minConnCacheSize:   conf.MinConnCacheSize,

		listeners: make([]*poller, conf.NListener),
		pollers:   make([]*poller, conf.NPoller),
		connsUnix: make([]*Conn, MaxOpenFiles),

		trigger: time.NewTimer(timeForever),
		chTimer: make(chan struct{}),
	}

	g.initHandlers()
	if g.epollMod == syscall.EPOLLET {
		g.OnReadBufferAlloc(func(c *Conn) []byte {
			if c.ReadBuffer == nil {
				c.ReadBuffer = make([]byte, int(g.readBufferSize))
			}
			return c.ReadBuffer
		})
	}

	return g
}
