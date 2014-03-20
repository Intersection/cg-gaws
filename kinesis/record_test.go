package kinesis

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBytes(t *testing.T) {
	Convey("When I use Bytes() on a record with good data in it", t, func() {
		r := Record{Data: "SGVsbG8gV29ybGQ="}
		result, err := r.Bytes()
		Convey("The result is what I expect", func() {
			So(result, ShouldResemble, []byte("Hello World"))
		})
		Convey("And there is no error", func() {
			So(err, ShouldBeNil)
		})
	})
	Convey("When I use Bytes() on a record with bad data in it", t, func() {
		r := Record{Data: "BAD DATA :("}
		_, err := r.Bytes()
		Convey("There is an error", func() {
			So(err, ShouldNotBeNil)
		})
	})
}
