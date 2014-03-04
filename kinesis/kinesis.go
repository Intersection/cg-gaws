// Package kinesis provides a way to interact with the AWS Kinesis service.
package kinesis

import (
	"bytes"
	"encoding/base64"
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

// PutRecord puts data on a Kinesis stream. It returns an error if it fails.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html for more details.
func (s *Stream) PutRecord(partitionKey string, data []byte) error {
	url := s.Service.Endpoint

	encodedData := base64.StdEncoding.EncodeToString(data)

	body := Record{StreamName: s.Name, Data: encodedData, PartitionKey: partitionKey}
	bodyAsJson, err := json.Marshal(body)
	payload := bytes.NewReader(bodyAsJson)

	req, err := http.NewRequest("POST", url, payload)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.PutRecord")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	_, err = gaws.SendAWSRequest(req)

	return err
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

// Delete deletes a stream. It is calling the DeleteStream API call.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_DeleteStream.html for more details.
func (s *Stream) Delete() error {
	url := s.Service.Endpoint

	req, err := http.NewRequest("POST", url, nil)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.DeleteStream")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	_, err = gaws.SendAWSRequest(req)

	return err
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
}

type StreamDescription struct {
	HasMoreShards bool
	Shards        []Shard
	StreamARN     string
	StreamName    string
	StreamStatus  string
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
	url := s.Service.Endpoint

	body := streamDescriptionRequest{StreamName: s.Name}
	bodyAsJson, err := json.Marshal(body)
	payload := bytes.NewReader(bodyAsJson)

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return StreamDescription{}, err
	}

	req.Header.Set("X-Amz-Target", "Kinesis_20131202.DescribeStream")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := gaws.SendAWSRequest(req)
	if err != nil {
		return StreamDescription{}, err
	}

	err = json.Unmarshal(resp, &result)
	if err != nil {
		return StreamDescription{}, err
	}
	return result.StreamDescription, err
}
