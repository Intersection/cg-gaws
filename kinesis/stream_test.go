package kinesis

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

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

func TestDeleteStream(t *testing.T) {
	Convey("Given a Stream and a Server that responds with success to every request", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		Convey("There is no error when I call Stream.Delete()", func() {
			result := testStream.Delete()
			So(result, ShouldBeNil)
		})
	})
	Convey("Given a Stream and a Server that responds with an error to every request", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		Convey("There is an error when I call Stream.Delete()", func() {
			result := testStream.Delete()
			So(result, ShouldNotBeNil)
		})
	})
}

var testStreamDescription []byte = []byte(`{
  "StreamDescription": {
    "HasMoreShards": false,
    "Shards": [
      {
        "HashKeyRange": {
          "EndingHashKey": "113427455640312821154458202477256070484",
          "StartingHashKey": "0"
        },
        "SequenceNumberRange": {
          "EndingSequenceNumber": "21269319989741826081360214168359141376",
          "StartingSequenceNumber": "21267647932558653966460912964485513216"
        },
        "ShardId": "shardId-000000000000"
      },
      {
        "HashKeyRange": {
          "EndingHashKey": "226854911280625642308916404954512140969",
          "StartingHashKey": "113427455640312821154458202477256070485"
        },
        "SequenceNumberRange": {
          "StartingSequenceNumber": "21267647932558653966460912964485513217"
        },
        "ShardId": "shardId-000000000001"
      },
      {
        "HashKeyRange": {
          "EndingHashKey": "340282366920938463463374607431768211455",
          "StartingHashKey": "226854911280625642308916404954512140970"
        },
        "SequenceNumberRange": {
          "StartingSequenceNumber": "21267647932558653966460912964485513218"
        },
        "ShardId": "shardId-000000000002"
      }
    ],
    "StreamARN": "arn:aws:kinesis:us-east-1:052958737983:exampleStreamName",
    "StreamName": "exampleStreamName",
    "StreamStatus": "ACTIVE"
  }
}`)

func testDescribeStreamSuccess(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write(testStreamDescription)
}

func TestDescribeStream(t *testing.T) {
	Convey("When you call stream.Describe() on a stream with an endpoint that returns a StreamDescription", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testDescribeStreamSuccess))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}
		description, err := testStream.Describe()

		Convey("The result will not return an error", func() {
			So(err, ShouldBeNil)
		})
		Convey("The result will be a StreamDescription", func() {
			So(description, ShouldHaveSameTypeAs, StreamDescription{})
		})
		Convey("The result will look like the example", func() {
			result := streamDescriptionResult{}
			_ = json.Unmarshal(testStreamDescription, &result)
			exampleDescription := result.StreamDescription

			for i, _ := range exampleDescription.Shards {
				exampleDescription.Shards[i].stream = &testStream
			}

			So(description, ShouldResemble, exampleDescription)
		})
		Convey("The second shards StartingHashKey will be the same as the example", func() {

			So(description.Shards[1].HashKeyRange.StartingHashKey, ShouldEqual, "113427455640312821154458202477256070485")
		})
	})
	Convey("When you call stream.Describe() on a stream with an endpoint that returns errors", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}
		_, err := testStream.Describe()
		Convey("The result will return an error", func() {
			So(err, ShouldNotBeNil)
		})
	})
	Convey("When you call stream.Describe() on a stream with an endpoint that returns something that is not a StreamDescription", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}
		_, err := testStream.Describe()
		Convey("The result will return an error", func() {
			So(err, ShouldNotBeNil)
		})
	})
}

func TestMergeShards(t *testing.T) {
	Convey("Given a Stream and a Server that responds with success to every request", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		Convey("There is no error when I call Stream.MergeShards()", func() {
			result := testStream.MergeShards("foo", "bar")
			So(result, ShouldBeNil)
		})
	})
	Convey("Given a Stream and a Server that responds with an error to every request", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		Convey("There is an error when I call Stream.MergeShards()", func() {
			result := testStream.MergeShards("foo", "bar")
			So(result, ShouldNotBeNil)
		})
	})
}

func TestSplitShard(t *testing.T) {
	Convey("Given a Stream and a Server that responds with success to every request", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		Convey("There is no error when I call Stream.SplitShard()", func() {
			result := testStream.SplitShard("foo", "bar")
			So(result, ShouldBeNil)
		})
	})
	Convey("Given a Stream and a Server that responds with an error to every request", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		Convey("There is an error when I call Stream.SplitShard()", func() {
			result := testStream.SplitShard("foo", "bar")
			So(result, ShouldNotBeNil)
		})
	})
}
