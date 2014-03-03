package kinesis

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/controlgroup/gaws"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetEndpoint(t *testing.T) {
	Convey("getEndpoint finds the appropriate endpoint for Kinesis.", t, func() {

		Convey("getEndpoint returns https://kinesis.us-east-1.amazonaws.com in us-east-1", func() {
			s := Stream{Name: "foo", Region: "us-east-1"}
			expectedEp := "https://kinesis.us-east-1.amazonaws.com"
			endpoint, err := s.getEndpoint()
			So(endpoint, ShouldEqual, expectedEp)
			So(err, ShouldBeNil)
		})

		Convey("getEndpoint returns nothing and an error for a bogus region", func() {
			s := Stream{Name: "foo", Region: "zork-east-1"}
			endpoint, err := s.getEndpoint()
			So(endpoint, ShouldEqual, "")
			So(err, ShouldNotBeNil)
		})

		Convey("getEndpoint returns the default endpoint for a stream without a region", func() {
			s := Stream{Name: "foo"}
			endpoint, err := s.getEndpoint()
			defaultEndpoint := gaws.Regions[gaws.Region].Endpoints.Kinesis
			So(endpoint, ShouldEqual, defaultEndpoint)
			So(err, ShouldBeNil)
		})
	})
}

func testHTTP200(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func TestPutRecord(t *testing.T) {
	Convey("Given a test stream, some data, and a partitionkey string", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))

		testRegion := gaws.AWSRegion{Name: "test-east-1", Endpoints: gaws.Endpoints{Kinesis: ts.URL}}
		gaws.Regions[testRegion.Name] = testRegion

		testStream := Stream{Name: "foo", Region: "test-east-1"}

		data := []byte("Hello world!")
		key := "foo"

		ep, err := testStream.getEndpoint()
		So(err, ShouldBeNil)
		So(ep, ShouldEqual, ts.URL)

		Convey("Putting a record on that stream succeeds", func() {
			err := testStream.PutRecord(key, data)

			So(err, ShouldBeNil)
		})
		// defer ts.Close()
	})
}
