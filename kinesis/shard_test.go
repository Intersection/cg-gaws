package kinesis

import (
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var exampleGetShardIteratorResponse = []byte(`{"ShardIterator": "AAAAAAAAAAETYyAYzd665+8e0X7JTsASDM/Hr2rSwc0X2qz93iuA3udrjTH+ikQvpQk/1ZcMMLzRdAesqwBGPnsthzU0/CBlM/U8/8oEqGwX3pKw0XyeDNRAAZyXBo3MqkQtCpXhr942BRTjvWKhFz7OmCb2Ncfr8Tl2cBktooi6kJhr+djN5WYkB38Rr3akRgCl9qaU4dY="}`)

func testGetShardIteratorSuccess(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(200)
	w.Write(exampleGetShardIteratorResponse)
}

func TestGetShardIterator(t *testing.T) {
	Convey("Given a Shard and a server that responds to GetShardIterator requests", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testGetShardIteratorSuccess))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		testShard := Shard{ShardId: "TestShard", stream: &testStream}

		Convey("Using GetShardIterator with a ShardIteratorType and StartingSequenceNumber", func() {
			result, err := testShard.GetShardIterator("LATEST", "12345")
			Convey("Does not return an error", func() {
				So(err, ShouldBeNil)
			})
			Convey("Returns a shard iterator", func() {
				So(result, ShouldEqual, "AAAAAAAAAAETYyAYzd665+8e0X7JTsASDM/Hr2rSwc0X2qz93iuA3udrjTH+ikQvpQk/1ZcMMLzRdAesqwBGPnsthzU0/CBlM/U8/8oEqGwX3pKw0XyeDNRAAZyXBo3MqkQtCpXhr942BRTjvWKhFz7OmCb2Ncfr8Tl2cBktooi6kJhr+djN5WYkB38Rr3akRgCl9qaU4dY=")
			})
		})
	})
	Convey("Given a GetShardIterator request to a server that returns bad data", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testBadJson))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		testShard := Shard{ShardId: "TestShard", stream: &testStream}
		resp, err := testShard.GetShardIterator("LATEST", "12345")

		Convey("It should return an error", func() {
			So(err, ShouldNotBeNil)
		})
		Convey("And the result should be empty", func() {
			So(resp, ShouldEqual, "")
		})
	})
	Convey("Given a GetShardIterator request to a server that returns an error", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP404))
		ks := KinesisService{Endpoint: ts.URL}
		testStream := Stream{Name: "foo", Service: &ks}

		testShard := Shard{ShardId: "TestShard", stream: &testStream}
		resp, err := testShard.GetShardIterator("LATEST", "12345")

		Convey("It should return an error", func() {
			So(err, ShouldNotBeNil)
		})
		Convey("And the result should be empty", func() {
			So(resp, ShouldEqual, "")
		})
	})
}
