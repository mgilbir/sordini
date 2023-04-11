package sordini

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/mgilbir/sordini/protocol"
	log "github.com/sirupsen/logrus"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	ErrTopicExists            = errors.New("topic exists already")
	ErrInvalidArgument        = errors.New("no logger set")
	OffsetsTopicName          = "__consumer_offsets"
	OffsetsTopicNumPartitions = 50
)

type topicPartition struct {
	topic     string
	partition int32
}

// Broker represents a broker in a Jocko cluster, like a broker in a Kafka cluster.
type Broker struct {
	mu   sync.RWMutex
	addr string
	id   int32

	offsetMu sync.RWMutex
	offset   map[topicPartition]int64
	topics   map[string]interface{}

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex

	callbacks []Callback
}

// New is used to instantiate a new broker.
func NewBroker(addr string) (*Broker, error) {
	b := &Broker{
		id:         1,
		addr:       addr,
		offset:     make(map[topicPartition]int64),
		shutdownCh: make(chan struct{}),
		topics:     make(map[string]interface{}),
	}
	return b, nil
}

func (b *Broker) host() string {
	addrTokens := strings.Split(b.addr, ":")
	return addrTokens[0]
}

func (b *Broker) port() int32 {
	addrTokens := strings.Split(b.addr, ":")
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
			log.Debugf("broker/%d: request: %v", b.id, reqCtx)

			if reqCtx == nil {
				goto DONE
			}

			var res kmsg.Response

			switch req := reqCtx.req.(type) {
			case *kmsg.ProduceRequest:
				res = b.handleProduce(reqCtx, req)
			// case *kmsg.OffsetsRequest:
			// 	res = b.handleOffsets(reqCtx, req)
			case *kmsg.MetadataRequest:
				res = b.handleMetadata(reqCtx, req)
			case *kmsg.LeaderAndISRRequest:
				res = b.handleLeaderAndISR(reqCtx, req)
			case *kmsg.StopReplicaRequest:
				res = b.handleStopReplica(reqCtx, req)
			case *kmsg.UpdateMetadataRequest:
				res = b.handleUpdateMetadata(reqCtx, req)
			case *kmsg.ControlledShutdownRequest:
				res = b.handleControlledShutdown(reqCtx, req)
			case *kmsg.OffsetCommitRequest:
				res = b.handleOffsetCommit(reqCtx, req)
			case *kmsg.OffsetFetchRequest:
				res = b.handleOffsetFetch(reqCtx, req)
			case *kmsg.FindCoordinatorRequest:
				res = b.handleFindCoordinator(reqCtx, req)
			case *kmsg.JoinGroupRequest:
				res = b.handleJoinGroup(reqCtx, req)
			case *kmsg.HeartbeatRequest:
				res = b.handleHeartbeat(reqCtx, req)
			case *kmsg.LeaveGroupRequest:
				res = b.handleLeaveGroup(reqCtx, req)
			case *kmsg.SyncGroupRequest:
				res = b.handleSyncGroup(reqCtx, req)
			case *kmsg.DescribeGroupsRequest:
				res = b.handleDescribeGroups(reqCtx, req)
			case *kmsg.ListGroupsRequest:
				res = b.handleListGroups(reqCtx, req)
			case *kmsg.SASLHandshakeRequest:
				// res = b.handleSaslHandshake(reqCtx, req)
			case *kmsg.ApiVersionsRequest:
				res = b.handleAPIVersions(reqCtx, req)
			case *kmsg.CreateTopicsRequest:
				res = b.handleCreateTopic(reqCtx, req)
			default:
				log.Errorf("broker: unknown request type: %T", req)
				continue
			}

			responses <- &Context{
				conn:          reqCtx.conn,
				correlationID: reqCtx.correlationID,
				res:           res,
			}
		case <-ctx.Done():
			goto DONE
		}
	}
DONE:
	log.Debugf("broker/%d: run done", b.id)
}

// req handling.

// var apiVersions = &kmsg.APIVersionsResponse{APIVersions: kmsg.APIVersions}
var apiVersions = &kmsg.ApiVersionsResponse{Version: 5} //FIX

func (b *Broker) handleAPIVersions(ctx *Context, req *kmsg.ApiVersionsRequest) *kmsg.ApiVersionsResponse {
	return apiVersions
}

func (b *Broker) handleCreateTopic(ctx *Context, reqs *kmsg.CreateTopicsRequest) *kmsg.CreateTopicsResponse {
	res := kmsg.NewCreateTopicsResponse()
	res.Version = reqs.Version

	return &res
}

// func (b *Broker) handleOffsets(ctx *Context, req *kmsg.OffsetsRequest) *kmsg.OffsetsResponse {
// 	res := kmsg.NewOffsetsResponse()
// 	.APIVersion = req.Version
// 	res.Responses = make([]*kmsg.OffsetResponse, len(req.Topics))
// 	for i, t := range req.Topics {
// 		res.Responses[i] = new(kmsg.OffsetResponse)
// 		res.Responses[i].Topic = t.Topic
// 		res.Responses[i].PartitionResponses = make([]*kmsg.PartitionResponse, 0, len(t.Partitions))
// 		for _, p := range t.Partitions {
// 			pres := kmsg.NewPartitionResponse()
// 			pres.Partition = p.Partition
// 			offset := b.NewestOffset()
// 			pres.Offsets = []int64{offset}
// 			res.Responses[i].PartitionResponses = append(res.Responses[i].PartitionResponses, pres)
// 		}
// 	}
// 	return &res
// }

func (b *Broker) NewestOffset(topic string, partition int32) int64 {
	b.offsetMu.Lock()
	defer b.offsetMu.Unlock()
	tp := topicPartition{topic: topic, partition: partition}
	v := b.offset[tp] + 1
	b.offset[tp] = v
	return v
}

func (b *Broker) CurrentOffset(topic string, partition int32) int64 {
	b.offsetMu.RLock()
	defer b.offsetMu.RUnlock()
	tp := topicPartition{topic: topic, partition: partition}
	return b.offset[tp]
}

func (b *Broker) handleProduce(ctx *Context, req *kmsg.ProduceRequest) *kmsg.ProduceResponse {
	res := kmsg.NewPtrProduceResponse()
	res.Version = req.Version

	for _, td := range req.Topics {
		tres := kmsg.NewProduceResponseTopic()
		tres.Topic = td.Topic
		tres.Partitions = make([]kmsg.ProduceResponseTopicPartition, len(td.Partitions))
		for i, p := range td.Partitions {
			pres := kmsg.NewProduceResponseTopicPartition()
			pres.Partition = p.Partition
			pres.BaseOffset = b.CurrentOffset(td.Topic, p.Partition)
			tres.Partitions[i] = pres

			msgs, err := protocol.ExtractMessagePayload(p.Records)
			if err != nil {
				panic(err)
			}

			for _, msg := range msgs {
				msg.Offset = b.NewestOffset(td.Topic, p.Partition)
				for _, cb := range b.callbacks {
					cb(td.Topic, msg.Offset, p.Partition, msg.Key, msg.Value)
				}
			}
		}
		res.Topics = append(res.Topics, tres)
	}
	return res
}

func (b *Broker) handleMetadata(ctx *Context, req *kmsg.MetadataRequest) *kmsg.MetadataResponse {
	var topicMetadata []kmsg.MetadataResponseTopic

	for _, topic := range req.Topics {
		b.topics[*topic.Topic] = nil
	}

	for topic := range b.topics {
		resTopic := kmsg.NewMetadataResponseTopic()
		resTopic.Topic = &topic
		topicMetadata = append(topicMetadata, kmsg.MetadataResponseTopic{
			Topic: &topic,
			Partitions: []kmsg.MetadataResponseTopicPartition{
				kmsg.MetadataResponseTopicPartition{
					Partition: 0,
					Leader:    b.id,
					Replicas:  []int32{b.id},
					ISR:       []int32{b.id},
				},
			},
		},
		)
	}

	log.Debugf("broker/%d: metadata: %#v", b.id, req)

	brokers := []kmsg.MetadataResponseBroker{
		kmsg.MetadataResponseBroker{
			NodeID: b.id,
			Host:   b.host(),
			Port:   b.port(),
		},
	}

	res := &kmsg.MetadataResponse{
		ControllerID: b.id,
		Brokers:      brokers,
		Topics:       topicMetadata,
	}
	res.Version = req.Version
	return res
}

func (b *Broker) handleLeaderAndISR(ctx *Context, req *kmsg.LeaderAndISRRequest) *kmsg.LeaderAndISRResponse {
	res := &kmsg.LeaderAndISRResponse{
		Partitions: make([]kmsg.LeaderAndISRResponseTopicPartition, len(req.PartitionStates)),
	}
	res.Version = req.Version
	for i, p := range req.PartitionStates {
		res.Partitions[i] = kmsg.LeaderAndISRResponseTopicPartition{
			Partition: p.Partition, Topic: p.Topic}
	}
	return res
}

func (b *Broker) handleFindCoordinator(ctx *Context, req *kmsg.FindCoordinatorRequest) *kmsg.FindCoordinatorResponse {
	res := &kmsg.FindCoordinatorResponse{}
	res.Version = req.Version

	res.Coordinators = make([]kmsg.FindCoordinatorResponseCoordinator, 1)
	res.Coordinators[0].NodeID = b.id
	res.Coordinators[0].Host = b.host()
	res.Coordinators[0].Port = b.port()

	return res
}

func (b *Broker) handleJoinGroup(ctx *Context, r *kmsg.JoinGroupRequest) *kmsg.JoinGroupResponse {

	res := &kmsg.JoinGroupResponse{}
	res.Version = r.Version

	res.Generation = 0
	res.MemberID = r.MemberID

	return res
}

func (b *Broker) handleLeaveGroup(ctx *Context, r *kmsg.LeaveGroupRequest) *kmsg.LeaveGroupResponse {
	res := &kmsg.LeaveGroupResponse{}
	res.Version = r.Version

	return res
}

func (b *Broker) handleSyncGroup(ctx *Context, r *kmsg.SyncGroupRequest) *kmsg.SyncGroupResponse {
	res := &kmsg.SyncGroupResponse{}
	res.Version = r.Version

	return res
}

func (b *Broker) handleHeartbeat(ctx *Context, r *kmsg.HeartbeatRequest) *kmsg.HeartbeatResponse {
	res := &kmsg.HeartbeatResponse{}
	res.Version = r.Version

	return res
}

func (b *Broker) handleListGroups(ctx *Context, req *kmsg.ListGroupsRequest) *kmsg.ListGroupsResponse {
	res := kmsg.NewListGroupsResponse()
	res.Version = req.Version

	return &res
}

func (b *Broker) handleDescribeGroups(ctx *Context, req *kmsg.DescribeGroupsRequest) *kmsg.DescribeGroupsResponse {

	res := kmsg.NewDescribeGroupsResponse()
	res.Version = req.Version

	return &res
}

func (b *Broker) handleStopReplica(ctx *Context, req *kmsg.StopReplicaRequest) *kmsg.StopReplicaResponse {
	panic("not implemented: stop replica")
}

func (b *Broker) handleUpdateMetadata(ctx *Context, req *kmsg.UpdateMetadataRequest) *kmsg.UpdateMetadataResponse {
	panic("not implemented: update metadata")
}

func (b *Broker) handleControlledShutdown(ctx *Context, req *kmsg.ControlledShutdownRequest) *kmsg.ControlledShutdownResponse {
	panic("not implemented: controlled shutdown")
}

func (b *Broker) handleOffsetCommit(ctx *Context, req *kmsg.OffsetCommitRequest) *kmsg.OffsetCommitResponse {
	panic("not implemented: offset commit")
}

func (b *Broker) handleOffsetFetch(ctx *Context, req *kmsg.OffsetFetchRequest) *kmsg.OffsetFetchResponse {
	res := kmsg.NewOffsetFetchResponse()
	res.Version = req.Version

	return &res

}

// Leave is used to prepare for a graceful shutdown.
func (b *Broker) Leave() error {
	log.Infof("broker/%d: starting leave", b.id)

	return nil
}

func (b *Broker) Shutdown() error {
	log.Infof("broker/%d: shutting down broker", b.id)
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}
	b.shutdown = true
	close(b.shutdownCh)

	return nil
}
