/* To-be HTTP unit tests. There is no way to gracefully shutdown HTTP server, therefore this test cannot be made at this stage. */
package v2

import (
	"fmt"
	"io/ioutil"
	"loveoneanother.at/tiedot/db"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

const serverPrefix = "http://localhost:65500/"

type ApiResponse struct {
	status   int
	response []byte
}

func UseAPI(endpoint string, get map[string]string, post map[string]string) (apiResp ApiResponse, err error) {
	reqUrl := serverPrefix + endpoint + "?"
	if get != nil {
		for k, v := range get {
			reqUrl += url.QueryEscape(k) + "=" + url.QueryEscape(v) + "&"
		}
	}
	formVals := url.Values{}
	if post != nil {
		for k, v := range post {
			formVals.Add(k, v)
		}
	}
	httpResp, err := http.PostForm(reqUrl, formVals)
	if err != nil {
		return
	}
	apiResp.status = httpResp.StatusCode
	var httpRespContent []byte
	httpRespContent, err = ioutil.ReadAll(httpResp.Body)
	apiResp.response = httpRespContent
	return
}

func TestAPIV2(t *testing.T) {
	tmp := "/tmp/tiedot_apiv2_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	db, err := db.OpenDB(tmp)
	if err != nil {
		t.Fatal(err)
	}
}
