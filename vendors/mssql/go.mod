module github.com/gsoultan/gpool/vendors/mssql

go 1.26.5

require (
	github.com/gsoultan/gpool v0.5.0
	github.com/microsoft/go-mssqldb v1.10.0
)

require (
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)

// The vendor module tracks the core from the same commit during development.
// A replace directive in a dependency is ignored by consumers, so this only
// affects building and testing this module in place.
replace github.com/gsoultan/gpool => ../..
