module github.com/gsoultan/gpool/benchmarks

go 1.26.5

// pgx and pglogrepl are here, and only here, as the libraries gpool is measured
// against. gpool itself depends on neither, and a consumer of gpool never
// downloads this module.
require github.com/gsoultan/gpool v1.0.0

require (
	github.com/gsoultan/caduceus v0.1.0
	github.com/jackc/pglogrepl v0.0.0-20260401131349-e37c41485510
	github.com/jackc/pgx/v5 v5.10.0
)

require (
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)

replace github.com/gsoultan/gpool => ..
