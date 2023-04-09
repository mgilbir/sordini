package sordini

import (
	"context"
	"testing"

	"sync"

	"fmt"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/serf/testutil/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dynaport "github.com/travisjeffery/go-dynaport"
)

const (
	topic = "test_topic"
)

func NewTestServer(t *testing.T) *Server {
	ports := dynaport.Get(4)

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])

	s, err := NewServer(addr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestProduceConsume(t *testing.T) {
	s1 := NewTestServer(t)
	ctx1, cancel1 := context.WithCancel((context.Background()))
	defer cancel1()
	err := s1.Start(ctx1)
	require.NoError(t, err)
	// TODO: mv close into teardown
	defer s1.Shutdown()

	ch := make(chan []byte)
	cb := func(topic string, offset int64, partition int32, key []byte, msg []byte) {
		ch <- msg
	}

	s1.RegisterCallback(cb)

	config := sarama.NewConfig()
	config.ClientID = "produce-consume-test"
	config.Version = sarama.V0_10_0_0
	config.ChannelBufferSize = 1
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10

	brokers := []string{s1.Addr().String()}

	retry.Run(t, func(r *retry.R) {
		client, err := sarama.NewClient(brokers, config)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		defer client.Close()
	})

	var got []byte
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		got = <-ch
		wg.Done()
	}()

	bValue := []byte("Hello from Jocko!")

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	msgValue := sarama.ByteEncoder(bValue)
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: msgValue,
	})
	require.NoError(t, err)

	wg.Wait()
	assert.EqualValues(t, got, bValue)
}
