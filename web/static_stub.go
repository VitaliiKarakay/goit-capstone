//go:build !embedui

package web

import (
	"io/fs"
	"testing/fstest"
)

// Dist is empty when the UI bundle is not embedded.
var Dist fs.FS = fstest.MapFS{}
