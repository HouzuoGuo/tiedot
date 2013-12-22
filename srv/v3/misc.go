/* Miscellaneous function handlers. */
package v3

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
)

// Flush and close all data files and shutdown entire program.
func Shutdown(w http.ResponseWriter, r *http.Request) {
	V3Sync.Lock()
	defer V3Sync.Unlock()
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	V3DB.Close()
	os.Exit(0)
}

// Pause all activities and make a dump of entire database to another file system location.
func Dump(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var dest string
	if !Require(w, r, "dest", &dest) {
		return
	}
	// Note that symbol links are skipped!
	walkFun := func(currPath string, info os.FileInfo, err error) error {
		if info.IsDir() {
			// Calculate directory path at destination and create it
			relPath, err := filepath.Rel(V3DB.Dir, currPath)
			if err != nil {
				return err
			}
			destDir := path.Join(dest, relPath)
			if err := os.MkdirAll(destDir, 0700); err != nil {
				return err
			}
			tdlog.Printf("Dump created directory %s with permission 0700", destDir)
		} else {
			// Open the file to be copied (collection data/index)
			src, err := os.Open(currPath)
			if err != nil {
				return err
			}
			// Calculate file path at destination and create it
			relPath, err := filepath.Rel(V3DB.Dir, currPath)
			if err != nil {
				return err
			}
			destPath := path.Join(dest, relPath)
			destFile, err := os.Create(destPath)
			if err != nil {
				return err
			}
			// Copy from source to destination
			written, err := io.Copy(destFile, src)
			if err != nil {
				return err
			}
			tdlog.Printf("Dump create file %s with permission 666 (before umask), size is %d", destPath, written)
		}
		return nil
	}
	V3Sync.Lock()
	defer V3Sync.Unlock()
	V3DB.Flush()
	err := filepath.Walk(V3DB.Dir, walkFun)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
	}
}

// Return server memory statistics.
func MemStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	stats := new(runtime.MemStats)
	runtime.ReadMemStats(stats)
	resp, err := json.Marshal(stats)
	if err != nil {
		http.Error(w, "Cannot serialize MemStats to JSON.", 500)
		return
	}
	w.Write(resp)
}

// Return server protocol version number.
func Version(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("3"))
}
