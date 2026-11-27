module github.com/gsoultan/nextar/examples/gpoolproxy

go 1.27.4

require github.com/gsoultan/nextar v1.2.0

// The example tracks the working tree, not a published tag. A consumer of
// github.com/gsoultan/nextar never sees this module, so the replace never
// reaches anyone else's build.
replace github.com/gsoultan/nextar => ../..
