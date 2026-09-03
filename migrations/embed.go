package migrations

import "embed"

// Files contains the ordered SQL migrations applied by the control plane.
//
//go:embed *.sql
var Files embed.FS
