package web

import "embed"

// Assets holds the embedded UI.
//
//go:embed index.html assets/*
var Assets embed.FS
