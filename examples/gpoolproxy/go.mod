module github.com/gsoultan/gpool/examples/gpoolproxy

go 1.26.5

require (
	github.com/gsoultan/gpool v0.2.0
	github.com/jackc/pgx/v5 v5.10.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)

// The example tracks the working tree, not a published tag. A consumer of
// github.com/gsoultan/gpool never sees this module, so the replace never
// reaches anyone else's build.
replace github.com/gsoultan/gpool => ../..
