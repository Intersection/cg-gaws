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

// Stream is a Kinesis stream
type Stream struct {
	Name   string // The name of the stream
	Region string // The AWS region for this stream. Will use gaws.Region by default.
}

// getEndpoint returns the kinesis endpoint from the gaws.Regions map
func (s *Stream) getEndpoint() (string, error) {
	if s.Region == "" {
		s.Region = gaws.Region
	}

	endpoint := gaws.Regions[s.Region].Endpoints.Kinesis

	if endpoint == "" {
		err := gaws.AWSError{Type: "GawsNoEndpointForRegion", Message: "There is no Kinesis endpoint in this region"}
		return endpoint, err
	}

	return endpoint, nil
}

// PutRecord puts data on a Kinesis stream.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html for more details.
func (s *Stream) PutRecord(partitionKey string, data []byte) error {
	url, err := s.getEndpoint()
	if err != nil {
		return err
	}

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
