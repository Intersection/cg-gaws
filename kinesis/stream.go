package kinesis

import (
	"encoding/base64"
	"encoding/json"
)

// PutRecord puts data on a Kinesis stream. It returns an error if it fails.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html for more details.
func (s *Stream) PutRecord(partitionKey string, data []byte) error {

	encodedData := base64.StdEncoding.EncodeToString(data)

	body := putRecordRequest{StreamName: s.Name, Data: encodedData, PartitionKey: partitionKey}
	bodyAsJson, err := json.Marshal(body)

	req := s.Service.request()
	req.Body = bodyAsJson
	req.Headers["X-Amz-Target"] = "Kinesis_20131202.PutRecord"

	_, err = req.Do()

	return err
}

// Delete deletes a stream. It is calling the DeleteStream API call.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_DeleteStream.html for more details.
func (s *Stream) Delete() error {
	req := s.Service.request()

	req.Headers["X-Amz-Target"] = "Kinesis_20131202.DeleteStream"

	_, err := req.Do()

	return err
}

// StreamDescription is the description of a kinesis stream
type StreamDescription struct {
	HasMoreShards bool
	Shards        []Shard
	StreamARN     string
	StreamName    string
	StreamStatus  string // The status of the stream. May be CREATING, DELETING, ACTIVE, or UPDATING.
}

type streamDescriptionResult struct {
	StreamDescription StreamDescription
}

type streamDescriptionRequest struct {
	ExclusiveStartShardId string `json:",omitempty"`
	Limit                 int    `json:",omitempty"`
	StreamName            string
}

// Describe describes a stream. It is calling the DescribeStream API call.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStream.html for more details.
func (s *Stream) Describe() (StreamDescription, error) {
	result := streamDescriptionResult{}

	body := streamDescriptionRequest{StreamName: s.Name}
	bodyAsJson, err := json.Marshal(body)

	req := s.Service.request()
	req.Body = bodyAsJson
	req.Headers["X-Amz-Target"] = "Kinesis_20131202.DescribeStream"

	resp, err := req.Do()
	if err != nil {
		return StreamDescription{}, err
	}

	err = json.Unmarshal(resp, &result)
	if err != nil {
		return StreamDescription{}, err
	}

	for i, _ := range result.StreamDescription.Shards {
		result.StreamDescription.Shards[i].stream = s

	}
	return result.StreamDescription, err
}

type mergeShardsRequest struct {
	AdjacentShardToMerge string
	ShardToMerge         string
	StreamName           string
}

// MergeShards merges shards in a stream
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_MergeShards.html for more details.
func (s *Stream) MergeShards(shardToMerge string, adjacentShardToMerge string) error {

	body := mergeShardsRequest{StreamName: s.Name, ShardToMerge: shardToMerge, AdjacentShardToMerge: adjacentShardToMerge}
	bodyAsJson, err := json.Marshal(body)

	req := s.Service.request()
	req.Body = bodyAsJson
	req.Headers["X-Amz-Target"] = "Kinesis_20131202.MergeShards"

	_, err = req.Do()

	return err
}

type splitShardRequest struct {
	NewStartingHashKey string
	ShardToSplit       string
	StreamName         string
}

// SplitShards splits shards in a stream
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_SplitShard.html for more details.
func (s *Stream) SplitShard(shardToSplit string, newStartingHashKey string) error {

	body := splitShardRequest{StreamName: s.Name, ShardToSplit: shardToSplit, NewStartingHashKey: newStartingHashKey}
	bodyAsJson, err := json.Marshal(body)

	req := s.Service.request()
	req.Body = bodyAsJson
	req.Headers["X-Amz-Target"] = "Kinesis_20131202.SplitShard"

	_, err = req.Do()
	return err
}
