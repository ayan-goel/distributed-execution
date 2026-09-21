// Package migrations embeds the SQL shipped with this binary so deployment does
// not depend on a mutable working directory or an operator's current checkout.
package migrations

import "embed"

//go:embed *.up.sql
var Files embed.FS
