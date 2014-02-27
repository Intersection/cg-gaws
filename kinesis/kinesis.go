package kinesis

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/controlgroup/gaws"
)

type Record struct {
	StreamName   string
	Data         string
	PartitionKey string
}

type Stream struct {
	Name string
}

func (s *Stream) PutRecord(partitionKey string, data []byte) error {
	url := "https://kinesis.us-east-1.amazonaws.com"

	encodedData := base64.StdEncoding.EncodeToString(data)

	body := Record{StreamName: s.Name, Data: encodedData, PartitionKey: partitionKey}
	bodyAsJson, err := json.Marshal(body)
	payload := bytes.NewReader(bodyAsJson)

	req, err := http.NewRequest("POST", url, payload)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.PutRecord")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := gaws.SendAWSRequest(req)
	defer resp.Body.Close()
	return err
}