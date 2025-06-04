package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
)

func TestNewConnectionManager(t *testing.T) {
	tests := []struct {
		name    string
		metrics ServerMetrics
		logger  logger.Logger
		clock   raft.Clock
	}{
		{
			name:    "with all dependencies",
			metrics: newMockServerMetrics(),
			logger:  logger.NewNoOpLogger(),
			clock:   newMockClock(),
		},
		{
			name:    "with nil clock defaults to standard clock",
			metrics: newMockServerMetrics(),
			logger:  logger.NewNoOpLogger(),
			clock:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := NewConnectionManager(tt.metrics, tt.logger, tt.clock)
			if cm == nil {
				t.Fatal("Expected non-nil ConnectionManager")
			}

			var _ ConnectionManager = cm

			if cm.GetActiveConnections() != 0 {
				t.Errorf("Expected 0 active connections initially, got %d", cm.GetActiveConnections())
			}

			infos := cm.GetAllConnectionInfo()
			if len(infos) != 0 {
				t.Errorf("Expected empty connection info map initially, got %d entries", len(infos))
			}
		})
	}
}

func TestConnectionManager_OnConnect(t *testing.T) {
	metrics := newMockServerMetrics()
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	cm := NewConnectionManager(metrics, logger, clock)

	t.Run("single connection", func(t *testing.T) {
		addr := "192.168.1.1:12345"
		startTime := clock.Now()

		cm.OnConnect(addr)

		if cm.GetActiveConnections() != 1 {
			t.Errorf("Expected 1 active connection, got %d", cm.GetActiveConnections())
		}

		if metrics.getActiveConnections() != 1 {
			t.Errorf("Expected metrics to show 1 active connection, got %d", metrics.getActiveConnections())
		}

		infos := cm.GetAllConnectionInfo()
		if len(infos) != 1 {
			t.Fatalf("Expected 1 connection info, got %d", len(infos))
		}

		info, exists := infos[addr]
		if !exists {
			t.Fatalf("Expected connection info for %s", addr)
		}

		if info.RemoteAddr != addr {
			t.Errorf("Expected RemoteAddr %s, got %s", addr, info.RemoteAddr)
		}
		if !info.ConnectedAt.Equal(startTime) {
			t.Errorf("Expected ConnectedAt %v, got %v", startTime, info.ConnectedAt)
		}
		if !info.LastActive.Equal(startTime) {
			t.Errorf("Expected LastActive %v, got %v", startTime, info.LastActive)
		}
		if info.RequestCount != 0 {
			t.Errorf("Expected RequestCount 0, got %d", info.RequestCount)
		}
	})

	t.Run("multiple connections", func(t *testing.T) {
		cm = NewConnectionManager(newMockServerMetrics(), logger, clock)

		addrs := []string{
			"192.168.1.1:12345",
			"192.168.1.2:12346",
			"10.0.0.1:8080",
		}

		for i, addr := range addrs {
			cm.OnConnect(addr)
			expectedCount := i + 1
			if cm.GetActiveConnections() != expectedCount {
				t.Errorf("After connecting %s, expected %d active connections, got %d",
					addr, expectedCount, cm.GetActiveConnections())
			}
		}

		infos := cm.GetAllConnectionInfo()
		if len(infos) != len(addrs) {
			t.Errorf("Expected %d connection infos, got %d", len(addrs), len(infos))
		}

		for _, addr := range addrs {
			if _, exists := infos[addr]; !exists {
				t.Errorf("Expected connection info for %s", addr)
			}
		}
	})

	t.Run("duplicate connection", func(t *testing.T) {
		cm = NewConnectionManager(newMockServerMetrics(), logger, clock)

		addr := "192.168.1.1:12345"

		cm.OnConnect(addr)
		if cm.GetActiveConnections() != 1 {
			t.Errorf("Expected 1 active connection after first connect, got %d", cm.GetActiveConnections())
		}

		cm.OnConnect(addr)
		if cm.GetActiveConnections() != 1 {
			t.Errorf("Expected 1 active connection after duplicate connect, got %d", cm.GetActiveConnections())
		}
	})
}

func TestConnectionManager_OnDisconnect(t *testing.T) {
	metrics := newMockServerMetrics()
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	cm := NewConnectionManager(metrics, logger, clock)

	t.Run("disconnect existing connection", func(t *testing.T) {
		addr := "192.168.1.1:12345"

		cm.OnConnect(addr)
		if cm.GetActiveConnections() != 1 {
			t.Errorf("Expected 1 active connection after connect, got %d", cm.GetActiveConnections())
		}

		cm.OnDisconnect(addr)
		if cm.GetActiveConnections() != 0 {
			t.Errorf("Expected 0 active connections after disconnect, got %d", cm.GetActiveConnections())
		}

		if metrics.getActiveConnections() != 0 {
			t.Errorf("Expected metrics to show 0 active connections, got %d", metrics.getActiveConnections())
		}

		infos := cm.GetAllConnectionInfo()
		if len(infos) != 0 {
			t.Errorf("Expected empty connection info map after disconnect, got %d entries", len(infos))
		}
	})

	t.Run("disconnect non-existent connection", func(t *testing.T) {
		// Reset
		cm = NewConnectionManager(newMockServerMetrics(), logger, clock)

		addr := "192.168.1.1:12345"

		cm.OnDisconnect(addr)
		if cm.GetActiveConnections() != 0 {
			t.Errorf("Expected 0 active connections, got %d", cm.GetActiveConnections())
		}
	})

	t.Run("disconnect one of multiple connections", func(t *testing.T) {
		metrics = newMockServerMetrics()
		cm = NewConnectionManager(metrics, logger, clock)

		addrs := []string{
			"192.168.1.1:12345",
			"192.168.1.2:12346",
			"10.0.0.1:8080",
		}

		for _, addr := range addrs {
			cm.OnConnect(addr)
		}

		disconnectAddr := addrs[1]
		cm.OnDisconnect(disconnectAddr)

		expectedCount := len(addrs) - 1
		if cm.GetActiveConnections() != expectedCount {
			t.Errorf("Expected %d active connections after disconnect, got %d",
				expectedCount, cm.GetActiveConnections())
		}

		if metrics.getActiveConnections() != expectedCount {
			t.Errorf("Expected metrics to show %d active connections, got %d",
				expectedCount, metrics.getActiveConnections())
		}

		infos := cm.GetAllConnectionInfo()
		if len(infos) != expectedCount {
			t.Errorf("Expected %d connection infos, got %d", expectedCount, len(infos))
		}

		if _, exists := infos[disconnectAddr]; exists {
			t.Errorf("Did not expect connection info for disconnected address %s", disconnectAddr)
		}

		for _, addr := range addrs {
			if addr == disconnectAddr {
				continue
			}
			if _, exists := infos[addr]; !exists {
				t.Errorf("Expected connection info for %s", addr)
			}
		}
	})
}

func TestConnectionManager_OnRequest(t *testing.T) {
	metrics := newMockServerMetrics()
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	cm := NewConnectionManager(metrics, logger, clock)

	t.Run("request on existing connection", func(t *testing.T) {
		addr := "192.168.1.1:12345"
		connectTime := clock.Now()

		cm.OnConnect(addr)

		clock.Advance(5 * time.Second)
		requestTime := clock.Now()

		cm.OnRequest(addr)

		infos := cm.GetAllConnectionInfo()
		info := infos[addr]

		if !info.ConnectedAt.Equal(connectTime) {
			t.Errorf("Expected ConnectedAt to remain %v, got %v", connectTime, info.ConnectedAt)
		}
		if !info.LastActive.Equal(requestTime) {
			t.Errorf("Expected LastActive %v, got %v", requestTime, info.LastActive)
		}
		if info.RequestCount != 1 {
			t.Errorf("Expected RequestCount 1, got %d", info.RequestCount)
		}
	})

	t.Run("multiple requests on same connection", func(t *testing.T) {
		clock = newMockClock()
		cm = NewConnectionManager(newMockServerMetrics(), logger, clock)

		addr := "192.168.1.1:12345"
		cm.OnConnect(addr)

		requestCount := 5
		for i := 0; i < requestCount; i++ {
			clock.Advance(time.Second)
			cm.OnRequest(addr)
		}

		infos := cm.GetAllConnectionInfo()
		info := infos[addr]

		if info.RequestCount != int64(requestCount) {
			t.Errorf("Expected RequestCount %d, got %d", requestCount, info.RequestCount)
		}
	})

	t.Run("request on non-existent connection", func(t *testing.T) {
		cm = NewConnectionManager(newMockServerMetrics(), logger, clock)

		addr := "192.168.1.1:12345"

		cm.OnRequest(addr)

		if cm.GetActiveConnections() != 0 {
			t.Errorf("Expected 0 active connections, got %d", cm.GetActiveConnections())
		}
	})
}

func TestConnectionManager_GetActiveConnections(t *testing.T) {
	metrics := newMockServerMetrics()
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	cm := NewConnectionManager(metrics, logger, clock)

	if cm.GetActiveConnections() != 0 {
		t.Errorf("Expected 0 active connections initially, got %d", cm.GetActiveConnections())
	}

	addrs := []string{
		"192.168.1.1:12345",
		"192.168.1.2:12346",
		"10.0.0.1:8080",
	}

	for i, addr := range addrs {
		cm.OnConnect(addr)
		expectedCount := i + 1
		if cm.GetActiveConnections() != expectedCount {
			t.Errorf("Expected %d active connections, got %d", expectedCount, cm.GetActiveConnections())
		}
	}

	for i, addr := range addrs {
		cm.OnDisconnect(addr)
		expectedCount := len(addrs) - i - 1
		if cm.GetActiveConnections() != expectedCount {
			t.Errorf("Expected %d active connections after disconnect, got %d",
				expectedCount, cm.GetActiveConnections())
		}
	}
}

func TestConnectionManager_GetAllConnectionInfo(t *testing.T) {
	metrics := newMockServerMetrics()
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	cm := NewConnectionManager(metrics, logger, clock)

	t.Run("empty initially", func(t *testing.T) {
		infos := cm.GetAllConnectionInfo()
		if len(infos) != 0 {
			t.Errorf("Expected empty connection info map initially, got %d entries", len(infos))
		}
	})

	t.Run("returns copy of connection info", func(t *testing.T) {
		addr := "192.168.1.1:12345"
		connectTime := clock.Now()
		cm.OnConnect(addr)

		infos1 := cm.GetAllConnectionInfo()
		if len(infos1) != 1 {
			t.Fatalf("Expected 1 connection info, got %d", len(infos1))
		}

		delete(infos1, addr)

		infos2 := cm.GetAllConnectionInfo()
		if len(infos2) != 1 {
			t.Errorf("Expected original connection info to be unmodified, got %d entries", len(infos2))
		}

		info, exists := infos2[addr]
		if !exists {
			t.Fatalf("Expected connection info for %s", addr)
		}

		if info.RemoteAddr != addr {
			t.Errorf("Expected RemoteAddr %s, got %s", addr, info.RemoteAddr)
		}
		if !info.ConnectedAt.Equal(connectTime) {
			t.Errorf("Expected ConnectedAt %v, got %v", connectTime, info.ConnectedAt)
		}
	})

	t.Run("multiple connections", func(t *testing.T) {
		clock = newMockClock()
		cm = NewConnectionManager(newMockServerMetrics(), logger, clock)

		addrs := []string{
			"192.168.1.1:12345",
			"192.168.1.2:12346",
			"10.0.0.1:8080",
		}

		connectTimes := make(map[string]time.Time)
		for _, addr := range addrs {
			clock.Advance(time.Second)
			connectTime := clock.Now()
			connectTimes[addr] = connectTime
			cm.OnConnect(addr)
		}

		infos := cm.GetAllConnectionInfo()
		if len(infos) != len(addrs) {
			t.Errorf("Expected %d connection infos, got %d", len(addrs), len(infos))
		}

		for _, addr := range addrs {
			info, exists := infos[addr]
			if !exists {
				t.Errorf("Expected connection info for %s", addr)
				continue
			}

			if info.RemoteAddr != addr {
				t.Errorf("Expected RemoteAddr %s, got %s", addr, info.RemoteAddr)
			}
			if !info.ConnectedAt.Equal(connectTimes[addr]) {
				t.Errorf("Expected ConnectedAt %v for %s, got %v",
					connectTimes[addr], addr, info.ConnectedAt)
			}
		}
	})
}

func TestConnectionManager_ConcurrentAccess(t *testing.T) {
	metrics := newMockServerMetrics()
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	cm := NewConnectionManager(metrics, logger, clock)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperationsPerGoroutine := 100

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numOperationsPerGoroutine {
				addr := fmt.Sprintf("192.168.1.%d:%d", id, 12345+j)
				cm.OnConnect(addr)
			}
		}(i)
	}

	wg.Wait()

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numOperationsPerGoroutine {
				addr := fmt.Sprintf("192.168.1.%d:%d", id, 12345+j)
				cm.OnRequest(addr)
			}
		}(i)
	}

	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numOperationsPerGoroutine {
				cm.GetActiveConnections()
				cm.GetAllConnectionInfo()
			}
		}()
	}

	wg.Wait()

	expectedConnections := numGoroutines * numOperationsPerGoroutine
	if cm.GetActiveConnections() != expectedConnections {
		t.Errorf("Expected %d active connections after concurrent operations, got %d",
			expectedConnections, cm.GetActiveConnections())
	}

	infos := cm.GetAllConnectionInfo()
	if len(infos) != expectedConnections {
		t.Errorf("Expected %d connection infos after concurrent operations, got %d",
			expectedConnections, len(infos))
	}

	for _, info := range infos {
		if info.RequestCount != 1 {
			t.Errorf("Expected RequestCount = 1 for %s, got %d", info.RemoteAddr, info.RequestCount)
		}
	}
}

func TestConnectionManager_Interface(t *testing.T) {
	var _ ConnectionManager = &connectionManager{}
}
