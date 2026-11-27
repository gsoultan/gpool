// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

// owed is a reply the backend owes: the ParseComplete or CloseComplete of a
// Parse or Close, whether the client sent it or the proxy did, or the
// ReadyForQuery of a message that ends a Sync. They are kept in the order the
// messages were sent, which is the order the server answers them in.
//
// The proxy records what a Parse or Close does to the statements a backend
// holds as it sends it, because the messages behind it are decided on that
// view. The server may skip it, though: after an error it skips every message
// up to the Sync. A Parse or Close still owed when its Sync's ReadyForQuery
// arrives was skipped, and what it did to the record is undone.
type owed struct {
	// completion is the reply that says the message ran, or zero for the
	// ReadyForQuery that ends a Sync.
	completion byte
	// injected is a message the client did not send, whose reply it is not to
	// see.
	injected bool

	// name is the statement the message changed, if it changed one. before
	// and had are what the backend held under the name until then; ownBefore
	// and ownHad the client's own set, for the client's own Close and Parse.
	name      string
	changed   bool
	before    []byte
	had       bool
	mine      bool
	ownBefore []byte
	ownHad    bool
}

// undo puts the record back as it was before the message was sent.
func (o *owed) undo(backend, own *statements) {
	if o.changed {
		put(backend, o.name, o.before, o.had)
	}
	if o.mine {
		put(own, o.name, o.ownBefore, o.ownHad)
	}
}

// put makes a set hold message under name, or nothing if it did not.
func put(set *statements, name string, message []byte, had bool) {
	if had {
		set.restore(name, message)
		return
	}
	set.forget(name)
}
