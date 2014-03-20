package kinesis

import (
	"encoding/json"
)

// Shard is a shard in a Kinesis stream.
type Shard struct {
	AdjacentParentShardId string
	HashKeyRange          struct {
		EndingHashKey   string
		StartingHashKey string
	}
	ParentShardId       string
	SequenceNumberRange struct {
		EndingSequenceNumber   string
		StartingSequenceNumber string
	}
	ShardId string
	stream  *Stream
}

type getShardIteratorResponse struct {
	ShardIterator string
}

type getShardIteratorRequest struct {
	ShardId                string
	ShardIteratorType      string
	StartingSequenceNumber string `json:",omitempty"`
	StreamName             string
}

// GetShardIterator gets a shard iterator from the shard. It takes a type, which is one of: AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, or LATEST and an optional sequence number to start on.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html for more details.
func (s *Shard) GetShardIterator(shardIteratorType string, startingSequenceNumber string) (string, error) {

	result := getShardIteratorResponse{}

	body := getShardIteratorRequest{ShardId: s.ShardId, ShardIteratorType: shardIteratorType, StartingSequenceNumber: startingSequenceNumber, StreamName: s.stream.Name}

	bodyAsJson, err := json.Marshal(body)
	req := s.stream.Service.request()

	req.Body = bodyAsJson
	req.Headers["X-Amz-Target"] = "Kinesis_20131202.GetShardIterator"

	resp, err := req.Do()
	if err != nil {
		return "", err
	}

	err = json.Unmarshal(resp, &result)
	if err != nil {
		return "", err
	}
	return result.ShardIterator, err
}
