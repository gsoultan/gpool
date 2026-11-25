// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

// Notification is one NotificationResponse, the server's half of LISTEN/NOTIFY.
type Notification struct {
	PID     uint32
	Channel string
	Payload string
}

// parseNotification reads a NotificationResponse body.
func parseNotification(body []byte) (Notification, bool) {
	if len(body) < 4 {
		return Notification{}, false
	}
	pid := be32(body)
	channel, rest, ok := cutString(body[4:])
	if !ok {
		return Notification{}, false
	}
	payload, _, ok := cutString(rest)
	return Notification{PID: pid, Channel: channel, Payload: payload}, ok
}
