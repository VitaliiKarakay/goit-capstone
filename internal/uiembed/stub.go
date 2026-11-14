//go:build !embedui

package uiembed

import "net/http"

// Handler returns nil when the UI bundle was not embedded at build time.
func Handler() http.Handler {
	return nil
}
