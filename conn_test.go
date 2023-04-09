package sordini

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mgilbir/sordini/config"
	"github.com/mgilbir/sordini/protocol"
)

const (
	tcp = "tcp"
)

func TestConn(t *testing.T) {
	s := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)
	err := s.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown()

	tests := []struct {
		name string
		fn   func(*testing.T, *Conn)
	}{
		{
			name: "close immediately",
			fn:   testConnClose,
		},
		{
			name: "create topice",
			fn:   testConnCreateTopic,
		},
		{
			name: "leader and isr",
			fn:   testConnLeaderAndISR,
		},
		{
			name: "alter configs",
			fn:   testConnAlterConfigs,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := (&Dialer{
				Resolver: &net.Resolver{},
			}).DialContext(ctx, tcp, s.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			test.fn(t, conn)
		})
	}
}

func testConnClose(t *testing.T, conn *Conn) {
	if err := conn.Close(); err != nil {
		t.Error(err)
	}
}

func testConnCreateTopic(t *testing.T, conn *Conn) {
	if _, err := conn.CreateTopics(&protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             "test_topic",
			NumPartitions:     4,
			ReplicationFactor: 1,
		}},
	}); err != nil {
		t.Error(err)
	}
}

func testConnLeaderAndISR(t *testing.T, conn *Conn) {
	if _, err := conn.LeaderAndISR(&protocol.LeaderAndISRRequest{
		ControllerID: 1,
		PartitionStates: []*protocol.PartitionState{{
			Topic:     "test_topic",
			Partition: 1,
			Leader:    1,
			ISR:       []int32{1},
			Replicas:  []int32{1},
		}},
	}); err != nil {
		t.Error(err)
	}
}

func testConnAlterConfigs(t *testing.T, conn *Conn) {
	t.Skip()

	val := "max"
	if _, err := conn.AlterConfigs(&protocol.AlterConfigsRequest{
		Resources: []protocol.AlterConfigsResource{
			{
				Type: 1,
				Name: "system",
				Entries: []protocol.AlterConfigsEntry{{
					Name:  "memory",
					Value: &val,
				}},
			},
		},
	}); err != nil {
		t.Error(err)
	}
}
