package cluster

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"github.com/pingcap/wormhole/pkg/etcdutil"
	"github.com/pingcap/wormhole/pkg/metapb"
)

var (
	ErrNoError = errors.New("not error")
)

const (
	keyPrefix = "/consumers/offset"
)

// OffsetStash allows to accumulate offsets and
// mark them as processed in a bulk
type OffsetStash struct {
	offsets map[topicPartition]offsetInfo
	mu      sync.Mutex
}

// NewOffsetStash inits a blank stash
func NewOffsetStash() *OffsetStash {
	return &OffsetStash{offsets: make(map[topicPartition]offsetInfo)}
}

// MarkOffset stashes the provided message offset
func (s *OffsetStash) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	s.MarkPartitionOffset(msg.Topic, msg.Partition, msg.Offset, metadata)
}

// MarkPartitionOffset stashes the offset for the provided topic/partition combination
func (s *OffsetStash) MarkPartitionOffset(topic string, partition int32, offset int64, metadata string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := topicPartition{Topic: topic, Partition: partition}
	if info := s.offsets[key]; offset >= info.Offset {
		info.Offset = offset
		info.Metadata = metadata
		s.offsets[key] = info
	}
}

// Offsets returns the latest stashed offsets by topic-partition
func (s *OffsetStash) Offsets() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]int64, len(s.offsets))
	for tp, info := range s.offsets {
		res[tp.String()] = info.Offset
	}
	return res
}

type OffsetCommitRequest struct {
	ConsumerGroup           string
	ConsumerGroupGeneration int32  // v1 or later
	ConsumerID              string // v1 or later
	RetentionTime           int64  // v2 or later

	blocks map[string]map[int32]*offsetCommitRequestBlock
}

func (r *OffsetCommitRequest) AddBlock(topic string, partitionID int32, offset int64, timestamp int64, metadata string) {
	if r.blocks == nil {
		r.blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	}

	if r.blocks[topic] == nil {
		r.blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
	}

	r.blocks[topic][partitionID] = &offsetCommitRequestBlock{offset, timestamp, metadata}
}

type offsetCommitRequestBlock struct {
	offset    int64
	timestamp int64
	metadata  string
}

type OffsetCommitResponse struct {
	Errors map[string]map[int32]error
}

func (r *OffsetCommitResponse) AddError(topic string, partition int32, err error) {
	if r.Errors == nil {
		r.Errors = make(map[string]map[int32]error)
	}
	partitions := r.Errors[topic]
	if partitions == nil {
		partitions = make(map[int32]error)
		r.Errors[topic] = partitions
	}
	partitions[partition] = err
}

type OffsetFetchRequest struct {
	ConsumerGroup string
	partitions    map[string][]int32
}

func (r *OffsetFetchRequest) AddPartition(topic string, partitionID int32) {
	if r.partitions == nil {
		r.partitions = make(map[string][]int32)
	}

	r.partitions[topic] = append(r.partitions[topic], partitionID)
}

type OffsetFetchResponseBlock struct {
	Offset   int64
	Metadata string
	Err      error
}

type OffsetFetchResponse struct {
	Blocks map[string]map[int32]*OffsetFetchResponseBlock
}

func (r *OffsetFetchResponse) GetBlock(topic string, partition int32) *OffsetFetchResponseBlock {
	if r.Blocks == nil {
		return nil
	}

	if r.Blocks[topic] == nil {
		return nil
	}

	return r.Blocks[topic][partition]
}

func (r *OffsetFetchResponse) AddBlock(topic string, partitionID int32, offset int64, timestamp int64, metadata string) {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock)
	}

	partitions := r.Blocks[topic]
	if partitions == nil {
		partitions = make(map[int32]*OffsetFetchResponseBlock)
		r.Blocks[topic] = partitions
	}

	r.Blocks[topic][partitionID] = &OffsetFetchResponseBlock{
		Offset:   offset,
		Metadata: metadata,
		Err:      nil,
	}
}

type etcdOffsetManager struct {
	client *etcdutil.Client
}

func newEtcdOffsetManager(client *etcdutil.Client) *etcdOffsetManager {
	return &etcdOffsetManager{client: client}
}

func (m *etcdOffsetManager) CommitOffset(request *OffsetCommitRequest) (*OffsetCommitResponse, error) {
	resp := new(OffsetCommitResponse)
	resp.Errors = make(map[string]map[int32]error)

	// TODO: concurrent write ?

	for topic, partions := range request.blocks {
		for partitionID, offset := range partions {
			p := &metapb.Partition{
				TopicName:   topic,
				PartitionId: partitionID,
				Pos: metapb.Position{
					KafkaOffset: &offset.offset,
					// TODO: the rest of position information
				},
			}
			prefix := fmt.Sprintf("%s/%s/%s/%d", keyPrefix, request.ConsumerGroup, topic, partitionID)
			err := m.client.SavePartition(prefix, p)
			resp.AddError(topic, partitionID, err)
		}
	}

	return resp, nil
}

func (m *etcdOffsetManager) FetchOffset(request *OffsetFetchRequest) (*OffsetFetchResponse, error) {
	partitions := &metapb.Partitions{}

	ok, err := m.client.LoadPartitions(keyPrefix, partitions)
	if err != nil {
		log.Errorf("load partitions err:%s, keyPrefix %s", err, keyPrefix)
		return nil, err
	}
	resp := new(OffsetFetchResponse)
	if !ok {
		// set default values if got nothing from etcd.
		for topic, partitions := range request.partitions {
			for _, p := range partitions {
				//set default value as sarama does.
				// &{Offset:-1 Metadata: Err:kafka server: Not an error, why are you printing me?}
				resp.AddBlock(topic, p, sarama.OffsetNewest, 0, "")
			}
		}
		return resp, nil
	}

	for _, p := range partitions.Partitions {
		resp.AddBlock(p.TopicName, p.PartitionId, p.Pos.GetKafkaOffset(), 0, "")
	}
	return resp, nil
}
