module github.com/gsoultan/nextar-engine/vendors/mysql

go 1.27.4

require (
	github.com/go-sql-driver/mysql v1.10.0
	github.com/gsoultan/nextar-engine v1.2.0
)

require filippo.io/edwards25519 v1.2.0 // indirect

// The vendor module tracks the core from the same commit during development.
// A replace directive in a dependency is ignored by consumers, so this only
// affects building and testing this module in place.
replace github.com/gsoultan/nextar-engine => ../..
