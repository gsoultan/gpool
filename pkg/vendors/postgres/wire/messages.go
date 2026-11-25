// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

// Backend message types, the first byte of every message the server sends.
const (
	MsgAuthentication       = 'R'
	MsgBackendKeyData       = 'K'
	MsgBindComplete         = '2'
	MsgCloseComplete        = '3'
	MsgCommandComplete      = 'C'
	MsgCopyBothResponse     = 'W'
	MsgCopyData             = 'd'
	MsgCopyDone             = 'c'
	MsgCopyInResponse       = 'G'
	MsgCopyOutResponse      = 'H'
	MsgDataRow              = 'D'
	MsgEmptyQueryResponse   = 'I'
	MsgErrorResponse        = 'E'
	MsgNegotiateProtocol    = 'v'
	MsgNoData               = 'n'
	MsgNoticeResponse       = 'N'
	MsgNotification         = 'A'
	MsgParameterDescription = 't'
	MsgParameterStatus      = 'S'
	MsgParseComplete        = '1'
	MsgPortalSuspended      = 's'
	MsgReadyForQuery        = 'Z'
	MsgRowDescription       = 'T'
)

// Transaction status reported by ReadyForQuery.
const (
	TxIdle   = 'I'
	TxActive = 'T'
	TxFailed = 'E'
)

// Format codes for parameters and result columns.
const (
	TextFormat   int16 = 0
	BinaryFormat int16 = 1
)

const (
	protocolVersion30 = 3 << 16
	sslRequestCode    = 80877103
	cancelRequestCode = 80877102
	headerSize        = 5
)
