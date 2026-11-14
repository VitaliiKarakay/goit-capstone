//go:build embedui

package uiembed

import (
	"io/fs"
	"net/http"
	"path"
	"strings"

	webroot "github.com/VitaliiKarakay/goit-capstone/web"
)

// Handler serves the production UI under /ui/.
func Handler() http.Handler {
	root, err := fs.Sub(webroot.Dist, "dist")
	if err != nil {
		return http.NotFoundHandler()
	}
	fileServer := http.FileServer(http.FS(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/ui/")
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" {
			serveSPA(w, root, "index.html")
			return
		}
		f, err := root.Open(path.Clean(rel))
		if err == nil {
			defer f.Close()
			if st, err := f.Stat(); err == nil && !st.IsDir() {
				r.URL.Path = "/" + rel
				fileServer.ServeHTTP(w, r)
				return
			}
		}
		serveSPA(w, root, "index.html")
	})
}

func serveSPA(w http.ResponseWriter, root fs.FS, name string) {
	data, err := fs.ReadFile(root, name)
	if err != nil {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}
