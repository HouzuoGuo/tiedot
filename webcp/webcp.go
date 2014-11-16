package webcp

import (
	"github.com/GeertJohan/go.rice"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/http"
)

var WebCp string

func RegisterWebCp() {
	if WebCp == "" || WebCp == "none" || WebCp == "no" || WebCp == "false" {
		tdlog.Noticef("Web control panel is disabled on your request")
		return
	}
	http.Handle("/"+WebCp+"/", http.StripPrefix("/"+WebCp, http.FileServer(rice.MustFindBox("static").HTTPBox())))
	tdlog.Noticef("Web control panel is accessible at /%s/", WebCp)
}
