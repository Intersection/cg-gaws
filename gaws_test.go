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

func testAWSThrottle(w http.ResponseWriter, r *http.Request) {
	b, _ := json.Marshal(throttlingError)

	w.WriteHeader(400)
	w.Write([]byte(b))
}

func TestSuccess(t *testing.T) {
	Convey("A successful request works", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		defer ts.Close()

		req, _ := http.NewRequest("GET", ts.URL, nil)

		resp, err := SendAWSRequest(req)

		Convey("There should be no errors", func() {
			So(err, ShouldBeNil)
		})

		Convey("The status code should be 200", func() {
			So(resp.StatusCode, ShouldEqual, 200)
		})

	})
}

func TestFailNoRetry(t *testing.T) {
	Convey("Requests that return 404 do not retry", t, func() {

		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		defer ts.Close()

		req, _ := http.NewRequest("GET", ts.URL, nil)

		resp, err := SendAWSRequest(req)

		Convey("SendAWSRequest should return an error", func() {
			So(err, ShouldNotBeNil)
		})

		Convey("SendAWSRequest should return a not found error", func() {
			So(err.Error(), ShouldEqual, notFoundError.Error())
		})

		Convey("The response status code should be 404", func() {
			So(resp.StatusCode, ShouldEqual, 404)
		})
	})
}

func TestThrottleRetry(t *testing.T) {
	Convey("Requests that return a 400 throttle error retry", t, func() {

		ts := httptest.NewServer(http.HandlerFunc(testAWSThrottle))
		defer ts.Close()

		req, _ := http.NewRequest("GET", ts.URL, nil)

		resp, err := SendAWSRequest(req)

		Convey("SendAWSRequest should return an error", func() {
			So(err, ShouldNotBeNil)
		})

		Convey("SendAWSRequest should return an exceeded retries error", func() {
			So(err.Error(), ShouldEqual, exceededRetriesError.Error())
		})

		Convey("The response status code should be 400", func() {
			So(resp.StatusCode, ShouldEqual, 400)
		})
	})
}
