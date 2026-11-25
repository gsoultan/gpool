module github.com/gsoultan/gpool/examples/gpoolproxy

go 1.26.5

require github.com/gsoultan/gpool v1.0.0

// The example tracks the working tree, not a published tag. A consumer of
// github.com/gsoultan/gpool never sees this module, so the replace never
// reaches anyone else's build.
replace github.com/gsoultan/gpool => ../..
