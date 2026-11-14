//go:build embedui

package web

import "embed"

// Dist holds the Vite production build (populated before go build -tags embedui).
//
//go:embed all:dist
var Dist embed.FS
