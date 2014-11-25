package webcp

import (
	"github.com/GeertJohan/go.rice"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/http"
)

func RegisterWebCp(routeName string) {
	http.Handle("/"+routeName+"/", http.StripPrefix("/"+routeName, http.FileServer(rice.MustFindBox("static").HTTPBox())))
	tdlog.Noticef("Web control panel is accessible at /%s/", routeName)
}
