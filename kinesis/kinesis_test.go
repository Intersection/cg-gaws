package kinesis

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func testHTTP200(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func testBadJson(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("{\"foo\":\"bar\""))
}

var notFoundError = kinesisError{Type: "NotFound", Message: "Could not find something"}

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
	Convey("Given a ListStreams request to a server that returns bad data", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testBadJson))
		ks := KinesisService{Endpoint: ts.URL}
		resp, err := ks.ListStreams()
		Convey("It should return an error", func() {
			So(err, ShouldNotBeNil)
		})
		Convey("And the result should be empty", func() {
			So(resp, ShouldResemble, []Stream{})
		})
	})
}

var testGetRecordsResult []byte = []byte(`{
  "NextShardIterator": "AAAAAAAAAAHsW8zCWf9164uy8Epue6WS3w6wmj4a4USt+CNvMd6uXQ+HL5vAJMznqqC0DLKsIjuoiTi1BpT6nW0LN2M2D56zM5H8anHm30Gbri9ua+qaGgj+3XTyvbhpERfrezgLHbPB/rIcVpykJbaSj5tmcXYRmFnqZBEyHwtZYFmh6hvWVFkIwLuMZLMrpWhG5r5hzkE=",
  "Records": [
    {
      "Data": "XzxkYXRhPl8w",
      "PartitionKey": "partitionKey",
      "SequenceNumber": "21269319989652663814458848515492872193"
    }
  ] 
}`)

func testGetRecordsSuccess(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write(testGetRecordsResult)
}

func TestGetRecords(t *testing.T) {

	Convey("When calling getRecords on a stream that returns records", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testGetRecordsSuccess))
		ks := KinesisService{Endpoint: ts.URL}

		records, nextIterator, err := ks.getRecords("foo", 0)

		Convey("It should not return an error", func() {
			So(err, ShouldBeNil)
		})

		Convey("It should return records and a shard iterator", func() {
			So(records[0].Data, ShouldEqual, "XzxkYXRhPl8w")
			So(nextIterator, ShouldEqual, "AAAAAAAAAAHsW8zCWf9164uy8Epue6WS3w6wmj4a4USt+CNvMd6uXQ+HL5vAJMznqqC0DLKsIjuoiTi1BpT6nW0LN2M2D56zM5H8anHm30Gbri9ua+qaGgj+3XTyvbhpERfrezgLHbPB/rIcVpykJbaSj5tmcXYRmFnqZBEyHwtZYFmh6hvWVFkIwLuMZLMrpWhG5r5hzkE=")
		})
	})
	Convey("When you call stream.Describe() on a stream with an endpoint that returns errors", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		ks := KinesisService{Endpoint: ts.URL}

		_, _, err := ks.getRecords("foo", 0)
		Convey("The result will return an error", func() {
			So(err, ShouldNotBeNil)
		})
	})
	Convey("When calling getRecords on a stream that returns an error", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		ks := KinesisService{Endpoint: ts.URL}

		_, _, err := ks.getRecords("foo", 0)
		Convey("It should return an error", func() {
			So(err, ShouldNotBeNil)
		})
	})
}

func TestStreamRecords(t *testing.T) {
	Convey("When StreamRecords is used on a service that returns a record", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testGetRecordsSuccess))
		ks := KinesisService{Endpoint: ts.URL}
		c, _ := ks.StreamRecords("foo")
		record := <-c
		Convey("The record will be what we expect", func() {
			So(record.Data, ShouldEqual, "XzxkYXRhPl8w")
		})
	})
	Convey("When StreamRecords is used on a service that returns an error", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		ks := KinesisService{Endpoint: ts.URL}
		_, e := ks.StreamRecords("foo")
		Convey("The error will be returned on the error channel", func() {
			So(e, ShouldNotBeNil)
		})
	})
}

func TestRetryPredicate(t *testing.T) {

	Convey("Given a response that is not JSON", t, func() {
		result, err := kinesisRetryPredicate(400, []byte("bad data"))

		Convey("RetryPredicate returns false", func() {
			So(result, ShouldBeFalse)
		})

		Convey("Error is not nil", func() {
			So(err, ShouldNotBeNil)
		})
	})

	Convey("Given a response that has a status of 500", t, func() {
		result, _ := kinesisRetryPredicate(500, []byte("{\"__type\": \"foo\",\"message\":\"bar\"}"))
		Convey("RetryPredicate returns true", func() {
			So(result, ShouldBeTrue)
		})
	})

	Convey("Given a response that is a \"Throttling\" type", t, func() {

		result, _ := kinesisRetryPredicate(400, []byte("{\"__type\": \"Throttling\",\"message\":\"bar\"}"))
		Convey("RetryPredicate returns true", func() {
			So(result, ShouldBeTrue)
		})
	})

	Convey("Given a response that is a \"ProvisionedThroughputExceededException\" type", t, func() {

		result, _ := kinesisRetryPredicate(400, []byte("{\"__type\": \"ProvisionedThroughputExceededException\",\"message\":\"bar\"}"))
		Convey("RetryPredicate returns true", func() {
			So(result, ShouldBeTrue)
		})
	})
}
