package cloudformation

import (
	"testing"
	"net/http"
	"net/http/httptest"
	//"github.com/controlgroup/gaws"
	. "github.com/smartystreets/goconvey/convey"
)

func testHTTP200(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func TestValidateTemplate(t *testing.T) {
	Convey("Given a test CloudFormation template", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(testHTTP200))
		url := CloudFormationService{Endpoint: ts.URL}
		t.Log(url)
	})
}
