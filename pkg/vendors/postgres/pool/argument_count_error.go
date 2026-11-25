// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import "fmt"

// ArgumentCountError reports a query given a different number of arguments
// than it has parameters. Nothing was sent; the connection is unaffected.
type ArgumentCountError struct {
	Want, Got int
}

func (e *ArgumentCountError) Error() string {
	return fmt.Sprintf("gpool/postgres: statement takes %d arguments, got %d", e.Want, e.Got)
}
