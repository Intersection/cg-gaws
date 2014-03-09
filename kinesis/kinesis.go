// Package kinesis provides a way to interact with the AWS Kinesis service.
package kinesis

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/controlgroup/gaws"
)

// Record is a Kinesis record. These are put onto Streams.
type Record struct {
	StreamName   string
	Data         string
	PartitionKey string
}

// KinesisService is the an alias for gaws.AWSService.
type KinesisService gaws.AWSService

// Stream is a Kinesis stream
type Stream struct {
	Name    string          // The name of the stream
	Service *KinesisService // The service for this region
}

// createStreamRequest is the request to the CreateStream API call.
type createStreamRequest struct {
	ShardCount int
	StreamName string
}

// CreateStream creates a new Kinesis stream. It returns a Stream and an error if it fails.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html for more details.
func (s *KinesisService) CreateStream(name string, shardCount int) (Stream, error) {

	stream := Stream{Name: name, Service: s}

	url := stream.Service.Endpoint

	body := createStreamRequest{StreamName: name, ShardCount: shardCount}

	bodyAsJson, err := json.Marshal(body)
	payload := bytes.NewReader(bodyAsJson)

	req, err := http.NewRequest("POST", url, payload)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.CreateStream")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	_, err = gaws.SendAWSRequest(req)

	return stream, err
}

// listStreamsResult is the result of the ListStreams API call
type listStreamsResult struct {
	HasMoreStreams bool
	StreamNames    []string
}

// BUG(drocamor): ListStreams does not retry if there are more streams. We should probably have ListStreams with args and ListAllStreams without them.

// ListStreams lists the Kinesis streams in an account. It returns a list of streams and an error if it fails.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListStreams.html for more details
func (s *KinesisService) ListStreams() ([]Stream, error) {

	url := s.Endpoint

	req, err := http.NewRequest("POST", url, nil)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.ListStreams")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	body, err := gaws.SendAWSRequest(req)

	if err != nil {
		return []Stream{}, err
	}

	result := listStreamsResult{}
	err = json.Unmarshal(body, &result)

	if err != nil {
		return []Stream{}, err
	}

	streams := make([]Stream, len(result.StreamNames))

	for i, name := range result.StreamNames {
		streams[i] = Stream{Name: name, Service: s}
	}

	return streams, nil
}

type HashKeyRange struct {
	EndingHashKey   string
	StartingHashKey string
}

type SequenceNumberRange struct {
	EndingSequenceNumber   string
	StartingSequenceNumber string
}

type Shard struct {
	AdjacentParentShardId string
	HashKeyRange          HashKeyRange
	ParentShardId         string
	SequenceNumberRange   SequenceNumberRange
	ShardId               string
	stream                *Stream
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
	url := s.stream.Service.Endpoint
	result := getShardIteratorResponse{}

	body := getShardIteratorRequest{ShardId: s.ShardId, ShardIteratorType: shardIteratorType, StartingSequenceNumber: startingSequenceNumber, StreamName: s.stream.Name}

	bodyAsJson, err := json.Marshal(body)
	payload := bytes.NewReader(bodyAsJson)

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return "", err
	}

	req.Header.Set("X-Amz-Target", "Kinesis_20131202.DescribeStream")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := gaws.SendAWSRequest(req)
	if err != nil {
		return "", err
	}

	err = json.Unmarshal(resp, &result)
	if err != nil {
		return "", err
	}
	return result.ShardIterator, err
}
