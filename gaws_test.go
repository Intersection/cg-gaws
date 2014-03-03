package gaws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var notFoundError = AWSError{Type: "NotFound", Message: "Could not find something"}
var throttlingError = AWSError{Type: "Throttling", Message: "You have been throttled"}

func testHTTP200(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func testHTTP404(w http.ResponseWriter, r *http.Request) {
	b, _ := json.Marshal(notFoundError)

	w.WriteHeader(404)
	w.Write([]byte(b))
}

func testHTTP404NonJson(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(404)
	w.Write([]byte("I am not JSON!"))
}

func testAWSThrottle(w http.ResponseWriter, r *http.Request) {
	b, _ := json.Marshal(throttlingError)

	w.WriteHeader(400)
	w.Write([]byte(b))
}

func TestSuccess(t *testing.T) {
	Convey("Given a request sent to a server that always returns 200s", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		defer ts.Close()

		req, _ := http.NewRequest("GET", ts.URL, nil)

		_, err := SendAWSRequest(req)

		Convey("SendAWSRequest will not return errors", func() {
			So(err, ShouldBeNil)
		})

	})
}

func TestFailBadJson(t *testing.T) {
	Convey("Given a server that returns 404 errors without JSON", t, func() {

		ts := httptest.NewServer(http.HandlerFunc(testHTTP404NonJson))
		defer ts.Close()

		req, _ := http.NewRequest("GET", ts.URL, nil)

		_, err := SendAWSRequest(req)

		Convey("SendAWSRequest should return an error", func() {
			So(err, ShouldNotBeNil)
		})

	})
}

func TestFailNoRetry(t *testing.T) {
	Convey("Given a server that returns 404 errors with proper JSON", t, func() {

		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		defer ts.Close()

		req, _ := http.NewRequest("GET", ts.URL, nil)

		_, err := SendAWSRequest(req)

		Convey("SendAWSRequest should return an error", func() {
			So(err, ShouldNotBeNil)
		})

		Convey("SendAWSRequest should return a not found error (and not attempt to retry)", func() {
			So(err.Error(), ShouldEqual, notFoundError.Error())
		})

	})
}

func TestThrottleRetry(t *testing.T) {
	Convey("Given a server that only returns 400 errors with the Trottle type", t, func() {

		ts := httptest.NewServer(http.HandlerFunc(testAWSThrottle))
		defer ts.Close()

		req, _ := http.NewRequest("GET", ts.URL, nil)

		_, err := SendAWSRequest(req)

		Convey("SendAWSRequest should return an error", func() {
			So(err, ShouldNotBeNil)
		})

		Convey("SendAWSRequest should return an exceeded retries error", func() {
			So(err.Error(), ShouldEqual, exceededRetriesError.Error())
		})

	})
}

func TestServiceFinder(t *testing.T) {
	Convey("Given a ServiceForRegion call with a valid region and service name", t, func() {
		service, err := ServiceForRegion("us-east-1", "kinesis")
		Convey("It will not return an error", func() {
			So(err, ShouldBeNil)
		})

		Convey("It will return the expected service", func() {
			expectedService := AWSService{Endpoint: "https://kinesis.us-east-1.amazonaws.com"}
			So(service, ShouldResemble, expectedService)
		})
	})
	Convey("Given a ServiceForRegion call with a valid region but invalid service name", t, func() {
		_, err := ServiceForRegion("us-east-1", "blahblah")
		Convey("It will return an error", func() {
			So(err, ShouldNotBeNil)
		})
	})
	Convey("Given a ServiceForRegion call with an invalid region and valid service name", t, func() {
		_, err := ServiceForRegion("blahblah", "kinesis")
		Convey("It will return an error", func() {
			So(err, ShouldNotBeNil)
		})
	})
}
