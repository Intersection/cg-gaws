// Package kinesis provides a way to interact with the AWS Kinesis service.
package kinesis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/controlgroup/gaws"
)

// kinesisError is the error document returned from the Kinesis service.
type kinesisError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// Error formats the kinesisError into an error message.
func (e kinesisError) Error() string {
	return fmt.Sprintf("%v: %v", e.Type, e.Message)
}

func kinesisRetryPredicate(status int, body []byte) (bool, error) {
	if status < 400 {
		return false, nil
	}

	// The request failed, but why?
	error := kinesisError{}

	err := json.Unmarshal(body, &error)
	if err != nil {
		return false, err
	}

	// retry if it is an AWS error
	if status >= 500 {
		return true, error
	}

	if error.Type == "Throttling" {
		return true, error
	}

	if error.Type == "ProvisionedThroughputExceededException" {
		return true, error
	}

	return false, error
}

func sendKinesisRequest(req *http.Request) ([]byte, error) {

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	result, err := gaws.SendAWSRequest(req, kinesisRetryPredicate)

	return result, err
}

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

	_, err = sendKinesisRequest(req)

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

	body, err := sendKinesisRequest(req)

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

// getRecordsRequest is used with GetRecords to request records from a stream. Limit is optional.
type getRecordsRequest struct {
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

// GetRecords returns one or more data records from a stream. limit can be an integer up to 10,000. If it is 0, this will use the default limit.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html for more details.
func (s *KinesisService) GetRecords(shardIterator string, limit int) ([]Record, string, error) {
	request := getRecordsRequest{ShardIterator: shardIterator, Limit: limit}
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

	resp, err := sendKinesisRequest(req)
	if err != nil {
		return []Record{}, "", err
	}

	err = json.Unmarshal(resp, &result)

	return result.Records, result.NextShardIterator, err

}

// BUG(drocamor): StreamRecords is a terrible name.

// StreamRecords creates a goroutine and uses GetRecords to send records over a channel. If it encounters an error, it will send the error over the error channel and exit the goroutine.
func (s *KinesisService) StreamRecords(shardIterator string) (<-chan Record, <-chan error) {
	c := make(chan Record)
	errc := make(chan error)
	go func() {
		for {
			records, newiterator, err := s.GetRecords(shardIterator, 0)

			if err != nil {
				errc <- err
				break
			}
			shardIterator = newiterator
			for _, r := range records {
				c <- r
			}
		}
	}()
	return c, errc
}
