package kinesis

import (
	// "encoding/json"
	// "net/http"
	// "net/http/httptest"
	"testing"

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
	})
}
