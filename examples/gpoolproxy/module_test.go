// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"os"
	"strings"
	"testing"
)

// The proxy speaks PostgreSQL's protocol itself, in both directions, and its
// tests do too. A driver that came back as a dependency — even only of a test —
// would be in go.mod before anybody noticed, so this is where it gets noticed.
//
// It used to be there twice over: first as the proxy's login to its server, and
// after that as the client its tests connected with.
//
// go.sum is held to the same rule by not existing. The module's one dependency
// is the working tree next to it, which needs no checksum, and nothing the
// proxy or its tests compile comes from anywhere else. The core module requires
// nothing either, so `go mod tidy` has no checksum to write; a go.sum that
// appears means a dependency did.
func TestModuleDependsOnNoPostgreSQLDriver(t *testing.T) {
	for _, file := range []string{"go.mod", "go.sum"} {
		content, err := os.ReadFile(file)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatalf("ReadFile(%s) = %v", file, err)
		}
		for _, driver := range []string{"jackc/", "lib/pq"} {
			if strings.Contains(string(content), driver) {
				t.Errorf("%s names %s; the proxy and its tests must not depend on a PostgreSQL driver "+
					"(if this is go.sum after a tidy, delete the file)", file, driver)
			}
		}
	}
}
