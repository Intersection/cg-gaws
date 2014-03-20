// Package gaws provides functions and variables that allow subpackages to work with AWS services.
package gaws

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"time"

	"github.com/smartystreets/go-aws-auth"
)

// MaxTries is the number of times to retry a failing AWS request.
var MaxTries int = 5

// gawsError is the error document returned from many AWS requests.
type gawsError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

var exceededRetriesError = gawsError{Type: "GawsExceededMaxRetries", Message: "The maximum number of retries for this request was exceeded."}

// Error formats the gawsError into an error message.
func (e gawsError) Error() string {
	return fmt.Sprintf("%v: %v", e.Type, e.Message)
}

type retryPredicate func(int, []byte) (bool, error)

// AWSRequest is a request to AWS. It is used instead of http.Request to facilitate retries.
type AWSRequest struct {
	RetryPredicate retryPredicate
	URL            string
	Method         string
	Headers        map[string]string
	Body           []byte
}

func (r *AWSRequest) getRequest() *http.Request {

	payload := bytes.NewReader(r.Body)
	req, _ := http.NewRequest(r.Method, r.URL, payload)

	for k, v := range r.Headers {
		req.Header.Set(k, v)
	}

	awsauth.Sign(req)
	return req
}

// Do makes the request to AWS and retries with an exponential backoff.
func (r *AWSRequest) Do() ([]byte, error) {
	client := &http.Client{}
	var lastBody []byte

	for try := 1; try < MaxTries; try++ {
		req := r.getRequest()
		resp, err := client.Do(req)

		if err != nil {
			return make([]byte, 0), err
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			return body, err
		}

		shouldRetry, err := r.RetryPredicate(resp.StatusCode, body)
		if shouldRetry {
			lastBody = body

			// Exponential backoff for the retry
			sleepDuration := time.Duration(100 * math.Pow(2.0, float64(try)))
			time.Sleep(sleepDuration * time.Millisecond)
		} else {
			return body, err
		}
	}
	return lastBody, exceededRetriesError
}
