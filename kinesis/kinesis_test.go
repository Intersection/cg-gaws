package kinesis

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/controlgroup/gaws"
	. "github.com/smartystreets/goconvey/convey"
)

func testHTTP200(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func TestPutRecord(t *testing.T) {
	Convey("Given a test stream, some data, and a partitionkey string", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))

		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		data := []byte("Hello world!")
		key := "foo"

		ep := testStream.Service.Endpoint

		So(ep, ShouldEqual, ts.URL)

		Convey("Putting a record on that stream succeeds", func() {
			err := testStream.PutRecord(key, data)

			So(err, ShouldBeNil)
		})

	})
}

var notFoundError = gaws.AWSError{Type: "NotFound", Message: "Could not find something"}

func testHTTP404(w http.ResponseWriter, r *http.Request) {
	b, _ := json.Marshal(notFoundError)

	w.WriteHeader(404)
	w.Write([]byte(b))
}

func TestCreateStream(t *testing.T) {
	Convey("Given a name and a shard count", t, func() {
		streamName := "foo"
		shardCount := 5

		Convey("When CreateStream is run against a server that always returns 200", func() {
			ts := httptest.NewServer(http.HandlerFunc(testHTTP200))

			ks := KinesisService{Endpoint: ts.URL}

			result, err := ks.CreateStream(streamName, shardCount)

			Convey("It does not return an error", func() {
				So(err, ShouldBeNil)
			})
			Convey("It returns  a Stream", func() {
				So(result, ShouldHaveSameTypeAs, Stream{})
			})
		})
		Convey("When CreateStream is run against a server that always returns 404", func() {
			ts := httptest.NewServer(http.HandlerFunc(testHTTP404))

			ks := KinesisService{Endpoint: ts.URL}

			_, err := ks.CreateStream(streamName, shardCount)

			Convey("it returns an error", func() {

				So(err, ShouldNotBeNil)
			})
		})
	})
}

var aListStreamsResult = listStreamsResult{HasMoreStreams: false, StreamNames: []string{"foo", "bar", "baz"}}

func testListStreamsSuccess(w http.ResponseWriter, r *http.Request) {
	b, _ := json.Marshal(aListStreamsResult)

	w.WriteHeader(200)
	w.Write([]byte(b))
}

func TestListStreams(t *testing.T) {
	Convey("Given a ListStreams request to a server that returns streams", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testListStreamsSuccess))
		ks := KinesisService{Endpoint: ts.URL}
		result, err := ks.ListStreams()

		Convey("It should return a list of streams", func() {
			So(result, ShouldHaveSameTypeAs, []Stream{})
			Convey("And it should have 3 streams in it.", func() {
				So(len(result), ShouldEqual, 3)
			})
		})

		Convey("It should not return an error", func() {
			So(err, ShouldBeNil)
		})
	})
	Convey("Given a ListStreams request to a server that returns an error", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		ks := KinesisService{Endpoint: ts.URL}
		_, err := ks.ListStreams()
		Convey("It should return an error", func() {
			So(err, ShouldNotBeNil)
		})
	})
}
