// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import "fmt"

// serverError is an ErrorResponse from PostgreSQL, kept as its fields so that
// callers classify it by SQLSTATE. Servers localise the message.
type serverError struct {
	Severity string
	Code     string
	Message  string
}

func (e *serverError) Error() string {
	return fmt.Sprintf("gpoolproxy: server reported %s: %s (SQLSTATE %s)", e.Severity, e.Message, e.Code)
}

// parseServerError reads the fields of an ErrorResponse body.
func parseServerError(body []byte) *serverError {
	failure := &serverError{}
	for len(body) > 1 {
		kind := body[0]
		value, rest, ok := cutCString(body[1:])
		if !ok {
			break
		}
		switch kind {
		case 'S':
			failure.Severity = value
		case 'C':
			failure.Code = value
		case 'M':
			failure.Message = value
		}
		body = rest
	}
	return failure
}
