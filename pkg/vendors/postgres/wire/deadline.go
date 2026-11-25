// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"context"
	"time"
)

// awaitExpiry waits for ctx to report the deadline a connection has just hit.
//
// The connection's deadline and the context's timer are set to the same
// instant, but the socket notices first. Without this, an exchange could fail
// with DeadlineExceeded while ctx.Err() still returned nil, and a caller that
// checks ctx.Err() to tell its own cancellation from a real failure would count
// the end of its run as an error. The wait is only taken once the deadline has
// passed, when the timer is due, so it is never longer than a scheduling delay.
func awaitExpiry(ctx context.Context) {
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		<-ctx.Done()
	}
}
