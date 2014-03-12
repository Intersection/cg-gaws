// Package kinesis provides a way to interact with the AWS Kinesis service.
package kinesis

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/controlgroup/gaws"
)

// PutRecord puts data on a Kinesis stream. It returns an error if it fails.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html for more details.
func (s *Stream) PutRecord(partitionKey string, data []byte) error {
	url := s.Service.Endpoint

	encodedData := base64.StdEncoding.EncodeToString(data)

	body := putRecordRequest{StreamName: s.Name, Data: encodedData, PartitionKey: partitionKey}
	bodyAsJson, err := json.Marshal(body)
	payload := bytes.NewReader(bodyAsJson)

	req, err := http.NewRequest("POST", url, payload)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.PutRecord")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	_, err = gaws.SendAWSRequest(req)

	return err
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

// GetRecordsResponse is returned by GetRecords.
type GetRecordsResponse struct {
	NextShardIterator string   // The next position in the shard from which to start sequentially reading data records.
	Records           []Record // A slice of Record structs
}

// GetRecords returns one or more data records from a shard.
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html for more details.
func (s *Stream) GetRecords(request GetRecordsRequest) (GetRecordsResponse, error) {
	result := GetRecordsResponse{}
	url := s.Service.Endpoint

	bodyAsJson, err := json.Marshal(request)

	if err != nil {
		return result, err
	}

	payload := bytes.NewReader(bodyAsJson)

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return result, err
	}

	req.Header.Set("X-Amz-Target", "Kinesis_20131202.GetRecords")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := gaws.SendAWSRequest(req)
	if err != nil {
		return result, err
	}

	err = json.Unmarshal(resp, &result)

	return result, err

}

type mergeShardsRequest struct {
	AdjacentShardToMerge string
	ShardToMerge         string
	StreamName           string
}

// MergeShards merges shards in a stream
// See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_MergeShards.html for more details.
func (s *Stream) MergeShards(shardToMerge string, adjacentShardToMerge string) error {
	url := s.Service.Endpoint

	body := mergeShardsRequest{StreamName: s.Name, ShardToMerge: shardToMerge, AdjacentShardToMerge: adjacentShardToMerge}
	bodyAsJson, err := json.Marshal(body)
	payload := bytes.NewReader(bodyAsJson)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return err
	}

	req.Header.Set("X-Amz-Target", "Kinesis_20131202.MergeShards")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	_, err = gaws.SendAWSRequest(req)

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
	url := s.Service.Endpoint

	body := splitShardRequest{StreamName: s.Name, ShardToSplit: shardToSplit, NewStartingHashKey: newStartingHashKey}
	bodyAsJson, err := json.Marshal(body)
	payload := bytes.NewReader(bodyAsJson)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return err
	}

	req.Header.Set("X-Amz-Target", "Kinesis_20131202.SplitShard")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	_, err = gaws.SendAWSRequest(req)

	return err
}
