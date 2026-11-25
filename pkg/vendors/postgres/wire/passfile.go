// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// passwordFromFile looks the connection up in a libpq password file, by default
// ~/.pgpass, and returns its password or the empty string.
//
// Like libpq, it ignores a file that anyone but its owner can read: a password
// file the whole host can read is not a secret, and using it would make that
// someone else's problem later.
func passwordFromFile(path string, c *Config) string {
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		path = filepath.Join(home, ".pgpass")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return ""
	}
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()

	host := c.Host
	if c.unix() {
		host = "localhost"
	}
	want := []string{host, strconv.Itoa(int(c.Port)), c.Database, c.User}

	lines := bufio.NewScanner(file)
	for lines.Scan() {
		line := lines.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := splitPassLine(line)
		if len(fields) == 5 && passLineMatches(fields[:4], want) {
			return fields[4]
		}
	}
	return ""
}

// splitPassLine splits on unescaped colons, removing the escapes.
func splitPassLine(line string) []string {
	var fields []string
	var field strings.Builder
	for i := 0; i < len(line); i++ {
		switch {
		case line[i] == '\\' && i+1 < len(line):
			i++
			field.WriteByte(line[i])
		case line[i] == ':':
			fields = append(fields, field.String())
			field.Reset()
		default:
			field.WriteByte(line[i])
		}
	}
	return append(fields, field.String())
}

func passLineMatches(fields, want []string) bool {
	for i, field := range fields {
		if field != "*" && field != want[i] {
			return false
		}
	}
	return true
}
