//go:build integration

package main

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type controlPartitionProxy struct {
	address     string
	mu          sync.Mutex
	partitioned bool
	connections map[net.Conn]net.Conn
	rejected    atomic.Int32
}

func newControlPartitionProxy(t *testing.T, upstream string) *controlPartitionProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &controlPartitionProxy{address: listener.Addr().String(), connections: make(map[net.Conn]net.Conn)}
	accepted := make(chan struct{})
	var forwarding sync.WaitGroup
	go func() {
		defer close(accepted)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			proxy.mu.Lock()
			if proxy.partitioned {
				proxy.rejected.Add(1)
				proxy.mu.Unlock()
				_ = client.Close()
				continue
			}
			proxy.mu.Unlock()
			server, err := net.DialTimeout("tcp", upstream, time.Second)
			if err != nil {
				_ = client.Close()
				continue
			}
			proxy.mu.Lock()
			// Recheck after dialing so a connection accepted before the cut cannot
			// restore control authority while the partition is active.
			if proxy.partitioned {
				proxy.rejected.Add(1)
				proxy.mu.Unlock()
				_ = client.Close()
				_ = server.Close()
				continue
			}
			proxy.connections[client] = server
			proxy.mu.Unlock()
			forwarding.Add(1)
			go func() {
				defer forwarding.Done()
				copied := make(chan struct{})
				// Forward opaque TLS bytes; certificate checks and RPC authorization
				// remain in the real worker and control server.
				go func() {
					_, _ = io.Copy(server, client)
					_ = server.Close()
					_ = client.Close()
					close(copied)
				}()
				_, _ = io.Copy(client, server)
				_ = client.Close()
				_ = server.Close()
				<-copied
				proxy.mu.Lock()
				delete(proxy.connections, client)
				proxy.mu.Unlock()
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		proxy.cut()
		<-accepted
		forwarding.Wait()
	})
	return proxy
}

func (p *controlPartitionProxy) cut() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitioned = true
	for client, server := range p.connections {
		_ = client.Close()
		_ = server.Close()
	}
	return len(p.connections)
}

func (p *controlPartitionProxy) restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitioned = false
}
