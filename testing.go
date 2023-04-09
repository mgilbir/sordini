package sordini

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mgilbir/sordini/config"
	"github.com/mitchellh/go-testing-interface"
	dynaport "github.com/travisjeffery/go-dynaport"
)

var (
	nodeNumber int32
)

func NewTestServer(t testing.T, cbBroker func(cfg *config.Config), cbServer func(cfg *config.Config)) *Server {
	ports := dynaport.Get(4)
	nodeID := atomic.AddInt32(&nodeNumber, 1)

	config := config.DefaultConfig()
	config.ID = nodeID
	config.NodeName = fmt.Sprintf("%s-node-%d", t.Name(), nodeID)
	config.Addr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	config.LeaveDrainTime = 1 * time.Millisecond
	config.ReconcileInterval = 300 * time.Millisecond

	if cbBroker != nil {
		cbBroker(config)
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("err != nil: %s", err)
	}

	if cbServer != nil {
		cbServer(config)
	}

	return NewServer(config, b, nil)
}
