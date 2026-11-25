// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// wireClient is the PostgreSQL client these tests drive the proxy with.
//
// It is the proxy's own code, not a client library: the login is the one the
// proxy uses towards its server, and everything after it is the handful of
// messages written out below. That keeps the module free of any driver, and it
// lets a test send exactly the bytes it means to — a batch without its Sync, a
// statement name chosen to collide — which a library exists to prevent.
//
// What it gives up is an independent implementation on the other end. The real
// server covers the client half of that, and TestProxyServesPsql the server
// half.
type wireClient struct {
	target *upstream
	login  *login
	conn   net.Conn
	reader *bufio.Reader

	// names maps SQL to the prepared statement it was parsed under. A nil map
	// means every query goes through the unnamed statement.
	names map[string]string

	// timeout bounds one exchange, so a proxy that loses a reply fails a test
	// instead of hanging it.
	timeout time.Duration
}

const clientTimeout = 60 * time.Second

// dialClient logs in to the server at url. extra is startup settings on top of
// the ones in the URL.
func dialClient(ctx context.Context, url string, extra ...[2]string) (*wireClient, error) {
	target, err := parseUpstream(url)
	if err != nil {
		return nil, err
	}
	session, err := target.connect(ctx, extra...)
	if err != nil {
		return nil, err
	}
	return &wireClient{
		target:  target,
		login:   session,
		conn:    session.conn,
		reader:  bufio.NewReaderSize(session.conn, readBufSize),
		timeout: clientTimeout,
	}, nil
}

// connect is dialClient for a test that cannot go on without the connection.
func connect(t *testing.T, url string) *wireClient {
	t.Helper()

	client, err := dialClient(t.Context(), url)
	if err != nil {
		t.Fatalf("dialClient() = %v", err)
	}
	t.Cleanup(client.close)
	return client
}

// caching makes the client prepare each distinct query once under a name of its
// own and bind that name afterwards, which is what client libraries do by
// default and what makes a pooler's life hard.
//
// The names are a counter, so two clients give the same name to different SQL
// as a matter of course.
func (c *wireClient) caching() *wireClient {
	c.names = map[string]string{}
	return c
}

// close says goodbye and hangs up. It is safe to call twice.
func (c *wireClient) close() {
	_ = c.conn.SetDeadline(time.Now().Add(time.Second))
	_, _ = c.conn.Write(appendMessage(nil, 'X'))
	_ = c.conn.Close()
}

// send writes the given messages as one write, so they reach the proxy
// together unless a test splits them on purpose.
func (c *wireClient) send(messages ...[]byte) error {
	var packet []byte
	for _, message := range messages {
		packet = append(packet, message...)
	}
	_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	_, err := c.conn.Write(packet)
	return err
}

// receive reads one message of any size.
func (c *wireClient) receive() (byte, []byte, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return 0, nil, err
	}
	length := int(binary.BigEndian.Uint32(header[1:])) - 4
	if length < 0 {
		return 0, nil, errors.New("message with a negative length")
	}
	body := make([]byte, length)
	_, err := io.ReadFull(c.reader, body)
	return header[0], body, err
}

// untilReady reads to the next ReadyForQuery and returns the rows on the way,
// as text. A server error is returned once the boundary is reached, never
// before, so the connection is always left usable.
func (c *wireClient) untilReady() ([][]string, error) {
	var rows [][]string
	var failure error
	for {
		kind, body, err := c.receive()
		if err != nil {
			return nil, err
		}
		switch kind {
		case 'D':
			rows = append(rows, decodeRow(body))
		case msgErrorResponse:
			if failure == nil {
				failure = parseServerError(body)
			}
		case 'Z':
			return rows, failure
		}
	}
}

// query runs one statement with its arguments bound server-side.
func (c *wireClient) query(sql string, args ...any) ([][]string, error) {
	if c.names == nil {
		if err := c.send(append(frontendBatch("", sql, args...), frontendSync())...); err != nil {
			return nil, err
		}
		return c.untilReady()
	}

	name, known := c.names[sql]
	if !known {
		name = "s" + strconv.Itoa(len(c.names)+1)
		if err := c.prepare(name, sql); err != nil {
			return nil, err
		}
		c.names[sql] = name
	}
	return c.execute(name, args...)
}

// value is query for the usual case of one row with one column.
func (c *wireClient) value(sql string, args ...any) (string, error) {
	rows, err := c.query(sql, args...)
	if err != nil {
		return "", err
	}
	return single(rows)
}

// number is value for a result that is an integer.
func (c *wireClient) number(sql string, args ...any) (int, error) {
	text, err := c.value(sql, args...)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(text)
}

// simple runs sql over the simple protocol, with no arguments.
func (c *wireClient) simple(sql string) ([][]string, error) {
	if err := c.send(frontendQuery(sql)); err != nil {
		return nil, err
	}
	return c.untilReady()
}

// prepare parses sql under a name the caller chose.
func (c *wireClient) prepare(name, sql string) error {
	if err := c.send(frontendParse(name, sql), frontendDescribe('S', name), frontendSync()); err != nil {
		return err
	}
	_, err := c.untilReady()
	return err
}

// execute binds and runs a statement prepared earlier.
func (c *wireClient) execute(name string, args ...any) ([][]string, error) {
	if err := c.send(frontendBind(name, args...), frontendExecute(), frontendSync()); err != nil {
		return nil, err
	}
	return c.untilReady()
}

// deallocate closes a prepared statement.
func (c *wireClient) deallocate(name string) error {
	if err := c.send(frontendClose('S', name), frontendSync()); err != nil {
		return err
	}
	_, err := c.untilReady()
	return err
}

// copyIn runs a COPY ... FROM STDIN and feeds it lines of text format until
// next reports there are no more.
func (c *wireClient) copyIn(sql string, next func() (string, bool)) error {
	if err := c.send(frontendQuery(sql)); err != nil {
		return err
	}
	for started := false; !started; {
		kind, body, err := c.receive()
		if err != nil {
			return err
		}
		switch kind {
		case 'G':
			started = true
		case msgErrorResponse:
			failure := parseServerError(body)
			_, _ = c.untilReady()
			return failure
		}
	}

	chunk := make([]byte, 0, readBufSize)
	for line, ok := next(); ok; line, ok = next() {
		chunk = append(chunk, line...)
		chunk = append(chunk, '\n')
		if len(chunk) < readBufSize/2 {
			continue
		}
		if err := c.send(appendMessage(nil, 'd', chunk)); err != nil {
			return err
		}
		chunk = chunk[:0]
	}
	if err := c.send(appendMessage(nil, 'd', chunk), appendMessage(nil, 'c')); err != nil {
		return err
	}
	_, err := c.untilReady()
	return err
}

// cancel asks for whatever this client is running to be cancelled, on a
// connection of its own as the protocol requires.
func (c *wireClient) cancel(ctx context.Context) error {
	return c.target.cancel(ctx, c.login.pid, c.login.secret)
}

// single returns the only value of a one-row, one-column result.
func single(rows [][]string) (string, error) {
	if len(rows) != 1 || len(rows[0]) != 1 {
		return "", fmt.Errorf("got %d rows, want one row of one value", len(rows))
	}
	return rows[0][0], nil
}

// decodeRow reads a DataRow of text values. NULL becomes the empty string,
// which no test here needs to tell apart.
func decodeRow(body []byte) []string {
	if len(body) < 2 {
		return nil
	}
	values := make([]string, 0, binary.BigEndian.Uint16(body))
	body = body[2:]
	for len(body) >= 4 {
		length := int(int32(binary.BigEndian.Uint32(body)))
		body = body[4:]
		if length < 0 {
			values = append(values, "")
			continue
		}
		values = append(values, string(body[:length]))
		body = body[length:]
	}
	return values
}

// sqlState is the SQLSTATE of a server error, or empty for any other failure.
func sqlState(err error) string {
	var failure *serverError
	if errors.As(err, &failure) {
		return failure.Code
	}
	return ""
}

// frontendBatch is everything that runs one statement except the Sync that
// ends it, so that a test can decide when the Sync goes.
func frontendBatch(name, sql string, args ...any) [][]byte {
	return [][]byte{frontendParse(name, sql), frontendBind(name, args...), frontendExecute()}
}

func frontendQuery(sql string) []byte {
	return appendMessage(nil, 'Q', terminated(sql))
}

func frontendParse(name, sql string) []byte {
	return appendMessage(nil, 'P', terminated(name), terminated(sql), []byte{0, 0})
}

// frontendBind binds the unnamed portal to a statement, with every argument and
// every result in text format.
func frontendBind(name string, args ...any) []byte {
	body := append(terminated(""), terminated(name)...)
	body = binary.BigEndian.AppendUint16(body, 0)
	body = binary.BigEndian.AppendUint16(body, uint16(len(args)))
	for _, arg := range args {
		text := argumentText(arg)
		body = binary.BigEndian.AppendUint32(body, uint32(len(text)))
		body = append(body, text...)
	}
	body = binary.BigEndian.AppendUint16(body, 0)
	return appendMessage(nil, 'B', body)
}

func frontendDescribe(kind byte, name string) []byte {
	return appendMessage(nil, 'D', []byte{kind}, terminated(name))
}

func frontendExecute() []byte {
	return appendMessage(nil, 'E', terminated(""), []byte{0, 0, 0, 0})
}

func frontendClose(kind byte, name string) []byte {
	return appendMessage(nil, 'C', []byte{kind}, terminated(name))
}

func frontendSync() []byte {
	return appendMessage(nil, 'S')
}

// argumentText is an argument in the text format the server parses.
func argumentText(arg any) string {
	switch arg := arg.(type) {
	case string:
		return arg
	case int:
		return strconv.Itoa(arg)
	case []byte:
		return `\x` + hex.EncodeToString(arg)
	}
	panic(fmt.Sprintf("wireClient cannot send a %T", arg))
}

// copyLine is one row of COPY text format. The values here never need
// escaping, and a test that sends one that does should say so loudly.
func copyLine(values ...string) string {
	line := strings.Join(values, "\t")
	if strings.ContainsAny(line, "\\\n\r") {
		panic("copyLine does not escape")
	}
	return line
}
