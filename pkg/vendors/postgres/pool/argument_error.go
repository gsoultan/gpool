// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import "github.com/gsoultan/caduceus"

// ArgumentError reports an argument that could not be encoded for its
// parameter. Nothing was sent; the connection is unaffected.
type ArgumentError = caduceus.ArgumentError

// ArgumentCountError reports a call with more or fewer arguments than the
// statement has parameters.
type ArgumentCountError = caduceus.ArgumentCountError
