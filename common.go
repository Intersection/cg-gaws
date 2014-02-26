package gaws

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/smartystreets/go-aws-auth"
)

// AWSError is the error document returned from many AWS requests.
type AWSError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// Error formats the AWSError into an error message.
func (e AWSError) Error() string {
	return fmt.Sprintf("%v: %v", e.Type, e.Message)
}

// SendAWSRequest signs and sends an AWS request.
func SendAWSRequest(req *http.Request) (*http.Response, error) {

	awsauth.Sign(req)
	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		return resp, err
	}

	if resp.StatusCode >= 400 {
		error := AWSError{}
		defer resp.Body.Close()
		errorbuf := new(bytes.Buffer)
		errorbuf.ReadFrom(resp.Body)

		err = json.Unmarshal(errorbuf.Bytes(), &error)
		if err != nil {
			return resp, err
		}

		if error.Type == "Throttling" {
			// retry
		}

		return resp, error

	}

	return resp, nil
}
