package sordini

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/mgilbir/sordini/config"
	"github.com/mgilbir/sordini/protocol"
	"github.com/pkg/errors"
)

var (
	ErrTopicExists            = errors.New("topic exists already")
	ErrInvalidArgument        = errors.New("no logger set")
	OffsetsTopicName          = "__consumer_offsets"
	OffsetsTopicNumPartitions = 50
)

// Broker represents a broker in a Jocko cluster, like a broker in a Kafka cluster.
type Broker struct {
	mu     sync.RWMutex
	config *config.Config

	logStateInterval time.Duration
	offsetMu         sync.RWMutex
	offset           int64
	topics           map[string]interface{}

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex

	callbacks []Callback
}

// New is used to instantiate a new broker.
func NewBroker(cfg *config.Config) (*Broker, error) {
	b := &Broker{
		config:           cfg,
		shutdownCh:       make(chan struct{}),
		logStateInterval: time.Millisecond * 250,
		topics:           make(map[string]interface{}),
	}
	return b, nil
}

func (b *Broker) host() string {
	addrTokens := strings.Split(b.config.Addr, ":")
	return addrTokens[0]
}

func (b *Broker) port() int32 {
	addrTokens := strings.Split(b.config.Addr, ":")
	port, err := strconv.ParseInt(addrTokens[1], 10, 32)
	if err != nil {
		log.Fatalf("failed to parse port: %v", err)
	}
	return int32(port)
}

// Broker API.

func (b *Broker) RegisterCallback(cb Callback) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.callbacks = append(b.callbacks, cb)
}

// Run starts a loop to handle requests send back responses.
func (b *Broker) Run(ctx context.Context, requests <-chan *Context, responses chan<- *Context) {
	for {
		select {
		case reqCtx := <-requests:
			log.Debugf("broker/%d: request: %v", b.config.ID, reqCtx)

			if reqCtx == nil {
				goto DONE
			}

			var res protocol.ResponseBody

			switch req := reqCtx.req.(type) {
			case *protocol.ProduceRequest:
				res = b.handleProduce(reqCtx, req)
			case *protocol.OffsetsRequest:
				res = b.handleOffsets(reqCtx, req)
			case *protocol.MetadataRequest:
				res = b.handleMetadata(reqCtx, req)
			case *protocol.LeaderAndISRRequest:
				res = b.handleLeaderAndISR(reqCtx, req)
			case *protocol.StopReplicaRequest:
				res = b.handleStopReplica(reqCtx, req)
			case *protocol.UpdateMetadataRequest:
				res = b.handleUpdateMetadata(reqCtx, req)
			case *protocol.ControlledShutdownRequest:
				res = b.handleControlledShutdown(reqCtx, req)
			case *protocol.OffsetCommitRequest:
				res = b.handleOffsetCommit(reqCtx, req)
			case *protocol.OffsetFetchRequest:
				res = b.handleOffsetFetch(reqCtx, req)
			case *protocol.FindCoordinatorRequest:
				res = b.handleFindCoordinator(reqCtx, req)
			case *protocol.JoinGroupRequest:
				res = b.handleJoinGroup(reqCtx, req)
			case *protocol.HeartbeatRequest:
				res = b.handleHeartbeat(reqCtx, req)
			case *protocol.LeaveGroupRequest:
				res = b.handleLeaveGroup(reqCtx, req)
			case *protocol.SyncGroupRequest:
				res = b.handleSyncGroup(reqCtx, req)
			case *protocol.DescribeGroupsRequest:
				res = b.handleDescribeGroups(reqCtx, req)
			case *protocol.ListGroupsRequest:
				res = b.handleListGroups(reqCtx, req)
			case *protocol.SaslHandshakeRequest:
				res = b.handleSaslHandshake(reqCtx, req)
			case *protocol.APIVersionsRequest:
				res = b.handleAPIVersions(reqCtx, req)
			case *protocol.CreateTopicRequests:
				res = b.handleCreateTopic(reqCtx, req)
			default:
				log.Errorf("broker: unknown request type: %T", req)
				continue
			}

			responses <- &Context{
				conn:   reqCtx.conn,
				header: reqCtx.header,
				res: &protocol.Response{
					CorrelationID: reqCtx.header.CorrelationID,
					Body:          res,
				},
			}
		case <-ctx.Done():
			goto DONE
		}
	}
DONE:
	log.Debugf("broker/%d: run done", b.config.ID)
}

// req handling.

var apiVersions = &protocol.APIVersionsResponse{APIVersions: protocol.APIVersions}

func (b *Broker) handleAPIVersions(ctx *Context, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	return apiVersions
}

func (b *Broker) handleCreateTopic(ctx *Context, reqs *protocol.CreateTopicRequests) *protocol.CreateTopicsResponse {
	res := new(protocol.CreateTopicsResponse)
	res.APIVersion = reqs.Version()

	return res
}

func (b *Broker) handleOffsets(ctx *Context, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {
	res := new(protocol.OffsetsResponse)
	res.APIVersion = req.Version()
	res.Responses = make([]*protocol.OffsetResponse, len(req.Topics))
	for i, t := range req.Topics {
		res.Responses[i] = new(protocol.OffsetResponse)
		res.Responses[i].Topic = t.Topic
		res.Responses[i].PartitionResponses = make([]*protocol.PartitionResponse, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			pres := new(protocol.PartitionResponse)
			pres.Partition = p.Partition
			offset := b.NewestOffset()
			pres.Offsets = []int64{offset}
			res.Responses[i].PartitionResponses = append(res.Responses[i].PartitionResponses, pres)
		}
	}
	return res
}

func (b *Broker) NewestOffset() int64 {
	b.offsetMu.Lock()
	defer b.offsetMu.Unlock()
	b.offset++
	return b.offset
}

func (b *Broker) handleProduce(ctx *Context, req *protocol.ProduceRequest) *protocol.ProduceResponse {
	res := new(protocol.ProduceResponse)
	res.APIVersion = req.Version()
	res.Responses = make([]*protocol.ProduceTopicResponse, len(req.TopicData))
	log.Debugf("broker/%d: produce: %#v", b.config.ID, req)
	for i, td := range req.TopicData {
		log.Debugf("broker/%d: produce to partition: %d: %v", b.config.ID, i, td)
		tres := make([]*protocol.ProducePartitionResponse, len(td.Data))
		for j, p := range td.Data {
			pres := &protocol.ProducePartitionResponse{}
			pres.Partition = p.Partition

			msgSet, err := ProcessRecordSet(p.RecordSet)
			if err != nil {
				log.Errorf("broker/%d: process record set: %s", b.config.ID, err)
				pres.ErrorCode = protocol.ErrUnknown.Code()
				continue
			}
			for _, cb := range b.callbacks {
				for _, msg := range msgSet.Messages {
					cb(td.Topic, b.NewestOffset(), p.Partition, msg.Key, msg.Value)
				}
			}

			tres[j] = pres
		}
		res.Responses[i] = &protocol.ProduceTopicResponse{
			Topic:              td.Topic,
			PartitionResponses: tres,
		}
	}
	return res
}

func ProcessRecordSet(data []byte) (*protocol.MessageSet, error) {
	log.Debug("process record set")
	d := protocol.NewDecoder(data)
	msgSet := new(protocol.MessageSet)
	err := msgSet.Decode(d)
	if err != nil {
		return nil, err
	}
	return msgSet, nil
}

func (b *Broker) handleMetadata(ctx *Context, req *protocol.MetadataRequest) *protocol.MetadataResponse {
	var topicMetadata []*protocol.TopicMetadata

	for _, topic := range req.Topics {
		b.topics[topic] = nil
	}

	for topic := range b.topics {
		topicMetadata = append(topicMetadata, &protocol.TopicMetadata{
			TopicErrorCode: protocol.ErrNone.Code(),
			Topic:          topic,
			PartitionMetadata: []*protocol.PartitionMetadata{
				&protocol.PartitionMetadata{
					PartitionErrorCode: protocol.ErrNone.Code(),
					PartitionID:        0,
					Leader:             b.config.ID,
					Replicas:           []int32{b.config.ID},
					ISR:                []int32{b.config.ID},
				},
			},
		},
		)
	}

	log.Debugf("broker/%d: metadata: %#v", b.config.ID, req)

	addrTokens := strings.Split(b.config.Addr, ":")
	port, err := strconv.ParseInt(addrTokens[1], 10, 32)
	if err != nil {
		panic(err)
	}

	brokers := []*protocol.Broker{
		&protocol.Broker{
			NodeID: b.config.ID,
			Host:   addrTokens[0],
			Port:   int32(port),
		},
	}

	res := &protocol.MetadataResponse{
		ControllerID:  b.config.ID,
		Brokers:       brokers,
		TopicMetadata: topicMetadata,
	}
	res.APIVersion = req.Version()
	return res
}

func (b *Broker) handleLeaderAndISR(ctx *Context, req *protocol.LeaderAndISRRequest) *protocol.LeaderAndISRResponse {
	res := &protocol.LeaderAndISRResponse{
		Partitions: make([]*protocol.LeaderAndISRPartition, len(req.PartitionStates)),
	}
	res.APIVersion = req.Version()
	for i, p := range req.PartitionStates {
		res.Partitions[i] = &protocol.LeaderAndISRPartition{
			Partition: p.Partition, Topic: p.Topic, ErrorCode: protocol.ErrNone.Code()}
	}
	return res
}

func (b *Broker) handleFindCoordinator(ctx *Context, req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	res := &protocol.FindCoordinatorResponse{}
	res.APIVersion = req.Version()

	res.Coordinator.NodeID = b.config.ID
	res.Coordinator.Host = b.host()
	res.Coordinator.Port = b.port()

	return res
}

func (b *Broker) handleJoinGroup(ctx *Context, r *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {

	res := &protocol.JoinGroupResponse{}
	res.APIVersion = r.Version()

	res.GenerationID = 0
	res.MemberID = r.MemberID

	return res
}

func (b *Broker) handleLeaveGroup(ctx *Context, r *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	res := &protocol.LeaveGroupResponse{}
	res.APIVersion = r.Version()

	return res
}

func (b *Broker) handleSyncGroup(ctx *Context, r *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	res := &protocol.SyncGroupResponse{}
	res.APIVersion = r.Version()

	return res
}

func (b *Broker) handleHeartbeat(ctx *Context, r *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	res := &protocol.HeartbeatResponse{}
	res.APIVersion = r.Version()
	res.ErrorCode = protocol.ErrNone.Code()

	return res
}

func (b *Broker) handleSaslHandshake(ctx *Context, req *protocol.SaslHandshakeRequest) *protocol.SaslHandshakeResponse {
	panic("not implemented: sasl handshake")
}

func (b *Broker) handleListGroups(ctx *Context, req *protocol.ListGroupsRequest) *protocol.ListGroupsResponse {
	res := new(protocol.ListGroupsResponse)
	res.APIVersion = req.Version()

	return res
}

func (b *Broker) handleDescribeGroups(ctx *Context, req *protocol.DescribeGroupsRequest) *protocol.DescribeGroupsResponse {

	res := new(protocol.DescribeGroupsResponse)
	res.APIVersion = req.Version()

	return res
}

func (b *Broker) handleStopReplica(ctx *Context, req *protocol.StopReplicaRequest) *protocol.StopReplicaResponse {
	panic("not implemented: stop replica")
}

func (b *Broker) handleUpdateMetadata(ctx *Context, req *protocol.UpdateMetadataRequest) *protocol.UpdateMetadataResponse {
	panic("not implemented: update metadata")
}

func (b *Broker) handleControlledShutdown(ctx *Context, req *protocol.ControlledShutdownRequest) *protocol.ControlledShutdownResponse {
	panic("not implemented: controlled shutdown")
}

func (b *Broker) handleOffsetCommit(ctx *Context, req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	panic("not implemented: offset commit")
}

func (b *Broker) handleOffsetFetch(ctx *Context, req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	res := new(protocol.OffsetFetchResponse)
	res.APIVersion = req.Version()
	res.Responses = make([]protocol.OffsetFetchTopicResponse, len(req.Topics))

	return res

}

// Leave is used to prepare for a graceful shutdown.
func (b *Broker) Leave() error {
	log.Infof("broker/%d: starting leave", b.config.ID)

	return nil
}

func (b *Broker) Shutdown() error {
	log.Infof("broker/%d: shutting down broker", b.config.ID)
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}
	b.shutdown = true
	close(b.shutdownCh)

	return nil
}
