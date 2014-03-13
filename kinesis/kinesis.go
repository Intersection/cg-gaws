// Package kinesis provides a way to interact with the AWS Kinesis service.
package kinesis

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/controlgroup/gaws"
)

// putRecordRequest is a Kinesis record. These are put onto Streams.
type putRecordRequest struct {
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

// GetRecordsRequest is used with GetRecords to request records from a stream. Limit is optional.
type GetRecordsRequest struct {
	Limit         int    `json:",omitempty"` // Optional number of records to return.
	ShardIterator string // The shard iterator to use.
}

// Record is a Kinesis record returned in a GetRecordsResponse.
type Record struct {
	Data           string // The data blob. It is Base64 encoded.
	PartitionKey   string // Identifies which shard in the stream the data record is assigned to.
	SequenceNumber string // The unique identifier for the record in the Amazon Kinesis stream.
}

// getRecordsResponse is returned by GetRecords.
type getRecordsResponse struct {
	NextShardIterator string   // The next position in the shard from which to start sequentially reading data records.
	Records           []Record // A slice of Record structs
}

// GetRecords returns one or more data records from a stream.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html for more details.
func (s *KinesisService) GetRecords(request GetRecordsRequest) ([]Record, string, error) {
	result := getRecordsResponse{}
	url := s.Endpoint

	bodyAsJson, err := json.Marshal(request)

	if err != nil {
		return []Record{}, "", err
	}

	payload := bytes.NewReader(bodyAsJson)

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return []Record{}, "", err
	}

	req.Header.Set("X-Amz-Target", "Kinesis_20131202.GetRecords")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := gaws.SendAWSRequest(req)
	if err != nil {
		return []Record{}, "", err
	}

	err = json.Unmarshal(resp, &result)

	return result.Records, result.NextShardIterator, err

}
