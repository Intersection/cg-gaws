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

type KinesisService gaws.AWSService

// Stream is a Kinesis stream
type Stream struct {
	Name   string // The name of the stream
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