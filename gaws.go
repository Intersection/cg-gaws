// Package gaws provides functions and variables that allow subpackages to work with AWS services.
package gaws

import (
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

type RetryPredicate func(int, []byte) (bool, error)

// SendAWSRequest signs and sends an AWS request.
// It will retry 500s and throttling errors with an exponential backoff.
func SendAWSRequest(req *http.Request, retry RetryPredicate) ([]byte, error) {

	awsauth.Sign(req)
	client := &http.Client{}
	var lastBody []byte

	for try := 1; try < MaxTries; try++ {

		resp, err := client.Do(req)
		defer resp.Body.Close()

		if err != nil {
			return make([]byte, 0), err
		}

		body, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			return body, err
		}

		shouldRetry, err := retry(resp.StatusCode, body)
		if shouldRetry {
			// Point lastBody to body
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
