// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// These tests script the server's side of a login by hand, with no database.
// A real server only ever plays the honest part; most of what a login has to
// get right is what it does when the peer does not.

const loginPassword = "correct horse"

// scriptedServer accepts one connection, hands it to script once the startup
// packet has been read, and returns a connection string for it.
func scriptedServer(t *testing.T, script func(conn net.Conn, reader *bufio.Reader, startup []byte)) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() = %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		var length [4]byte
		if _, err := io.ReadFull(reader, length[:]); err != nil {
			return
		}
		startup := make([]byte, binary.BigEndian.Uint32(length[:])-4)
		if _, err := io.ReadFull(reader, startup); err != nil {
			return
		}
		script(conn, reader, startup)
	}()

	return "postgres://app:" + strings.ReplaceAll(loginPassword, " ", "%20") + "@" + listener.Addr().String() + "/db?sslmode=disable"
}

// readPassword reads one 'p' message the client sent.
func readPassword(reader *bufio.Reader) ([]byte, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	if header[0] != msgPassword {
		return nil, errors.New("not a password message")
	}
	body := make([]byte, binary.BigEndian.Uint32(header[1:])-4)
	_, err := io.ReadFull(reader, body)
	return body, err
}

// ready is what an honest server sends once it is satisfied.
func ready() []byte {
	buf := appendAuthentication(nil, authOK, nil)
	buf = appendParameterStatus(buf, "server_version", "16.0")
	buf = appendBackendKeyData(buf, 4242, []byte{1, 2, 3, 4})
	return appendReadyForQuery(buf, txIdle)
}

func loginTo(t *testing.T, url string) (*login, error) {
	t.Helper()

	target, err := parseUpstream(url)
	if err != nil {
		t.Fatalf("parseUpstream() = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	established, err := target.connect(ctx)
	if err == nil {
		t.Cleanup(func() { established.conn.Close() })
	}
	return established, err
}

func TestLoginCollectsWhatTheServerSaysAboutItself(t *testing.T) {
	url := scriptedServer(t, func(conn net.Conn, _ *bufio.Reader, _ []byte) {
		conn.Write(ready())
		io.Copy(io.Discard, conn)
	})

	established, err := loginTo(t, url)
	if err != nil {
		t.Fatalf("connect() = %v", err)
	}
	if established.pid != 4242 || string(established.secret) != "\x01\x02\x03\x04" {
		t.Errorf("key = %d %v, want 4242 [1 2 3 4]", established.pid, established.secret)
	}
	if got := established.statuses["server_version"]; got != "16.0" {
		t.Errorf("server_version = %q, want 16.0", got)
	}
}

// TestLoginStartupPacketNamesUserDatabaseAndSettings pins the one message the
// login composes without a server to correct it. A wrong length or a missing
// terminator here is a server that hangs up without saying why.
func TestLoginStartupPacketNamesUserDatabaseAndSettings(t *testing.T) {
	target, err := parseUpstream("postgres://app:pw@db.example:6000/orders?application_name=proxy&sslmode=disable&search_path=s")
	if err != nil {
		t.Fatalf("parseUpstream() = %v", err)
	}
	packet := (&login{target: target, version: protocolVersion30}).startupMessage([][2]string{{"replication", "database"}})

	if got := int(binary.BigEndian.Uint32(packet)); got != len(packet) {
		t.Errorf("length field = %d, packet is %d bytes", got, len(packet))
	}
	if got := binary.BigEndian.Uint32(packet[4:]); got != protocolVersion30 {
		t.Errorf("protocol = %d, want %d", got, protocolVersion30)
	}
	want := "user\x00app\x00database\x00orders\x00application_name\x00proxy\x00search_path\x00s\x00replication\x00database\x00\x00"
	if got := string(packet[8:]); got != want {
		t.Errorf("parameters = %q\nwant         %q", got, want)
	}
}

// TestMD5ResponseMatchesPostgreSQL checks the hash against one PostgreSQL
// itself computed, for user u_md5, password md5-pw and salt 01 02 03 04:
//
//	SELECT 'md5' || md5(convert_to(md5('md5-pw' || 'u_md5'), 'UTF8') || '\x01020304'::bytea)
func TestMD5ResponseMatchesPostgreSQL(t *testing.T) {
	const want = "md5cbbe473445de1f4b86fc09cf6b6af394"
	if got := md5Response("u_md5", "md5-pw", []byte{1, 2, 3, 4}); got != want {
		t.Errorf("md5Response() = %q, want %q", got, want)
	}
}

func TestLoginAnswersAnMD5Challenge(t *testing.T) {
	answered := make(chan string, 1)
	url := scriptedServer(t, func(conn net.Conn, reader *bufio.Reader, _ []byte) {
		conn.Write(appendAuthentication(nil, authMD5, []byte{9, 8, 7, 6}))
		body, err := readPassword(reader)
		if err != nil {
			return
		}
		answered <- strings.TrimRight(string(body), "\x00")
		conn.Write(ready())
		io.Copy(io.Discard, conn)
	})

	if _, err := loginTo(t, url); err != nil {
		t.Fatalf("connect() = %v", err)
	}
	if got, want := <-answered, md5Response("app", loginPassword, []byte{9, 8, 7, 6}); got != want {
		t.Errorf("sent %q, want %q", got, want)
	}
}

// scramScript runs the server half of SCRAM using the proxy's own scramServer,
// with alter given the chance to spoil the server's final signature.
func scramScript(t *testing.T, alter func(signature []byte) []byte, skipFinal bool) func(net.Conn, *bufio.Reader, []byte) {
	credential, err := deriveVerifier(loginPassword, defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}

	return func(conn net.Conn, reader *bufio.Reader, _ []byte) {
		server := &scramServer{verifier: credential}
		conn.Write(appendAuthentication(nil, authSASL, append(terminated(scramMechanism), 0)))

		initial, err := readPassword(reader)
		if err != nil {
			return
		}
		// mechanism, a terminator, a four-byte length, then the message.
		first := initial[len(scramMechanism)+1+4:]
		challenge, err := server.challenge(first)
		if err != nil {
			return
		}
		conn.Write(appendAuthentication(nil, authSASLContinue, challenge))

		final, err := readPassword(reader)
		if err != nil {
			return
		}
		signature, err := server.verify(final)
		if err != nil {
			conn.Write(errorResponse("FATAL", "28P01", "password authentication failed"))
			return
		}
		if !skipFinal {
			conn.Write(appendAuthentication(nil, authSASLFinal, alter(signature)))
		}
		conn.Write(ready())
		io.Copy(io.Discard, conn)
	}
}

func unaltered(signature []byte) []byte { return signature }

func TestLoginCompletesSCRAM(t *testing.T) {
	established, err := loginTo(t, scriptedServer(t, scramScript(t, unaltered, false)))
	if err != nil {
		t.Fatalf("connect() = %v", err)
	}
	if !established.verified {
		t.Error("login succeeded without verifying the server's signature")
	}
}

// TestLoginRejectsAServerThatCannotProveItKnowsThePassword is the half of SCRAM
// that is easy to leave out, because a login works without it. A peer that
// merely accepts connections can replay a challenge and say "ok"; only the
// signature shows it held the verifier.
func TestLoginRejectsAServerThatCannotProveItKnowsThePassword(t *testing.T) {
	tests := []struct {
		name      string
		alter     func([]byte) []byte
		skipFinal bool
	}{
		{"wrong signature", func([]byte) []byte { return []byte("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=") }, false},
		{"no signature at all", unaltered, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loginTo(t, scriptedServer(t, scramScript(t, test.alter, test.skipFinal)))
			if !errors.Is(err, errSCRAM) {
				t.Fatalf("connect() = %v, want errSCRAM", err)
			}
		})
	}
}

// TestLoginWillNotSendThePasswordInTheClear covers a server, or something in
// its place, asking for the password itself on a connection anyone on the path
// can read. The password must not be written at all, not merely the login
// failed afterwards.
func TestLoginWillNotSendThePasswordInTheClear(t *testing.T) {
	received := make(chan int, 1)
	url := scriptedServer(t, func(conn net.Conn, reader *bufio.Reader, _ []byte) {
		conn.Write(appendAuthentication(nil, authCleartext, nil))
		n, _ := io.Copy(io.Discard, reader)
		received <- int(n)
	})

	if _, err := loginTo(t, url); !errors.Is(err, errCleartext) {
		t.Fatalf("connect() = %v, want errCleartext", err)
	}
	if n := <-received; n != 0 {
		t.Errorf("client wrote %d bytes after being asked for a cleartext password", n)
	}
}

func TestLoginReportsTheServersErrorByCode(t *testing.T) {
	url := scriptedServer(t, func(conn net.Conn, _ *bufio.Reader, _ []byte) {
		conn.Write(errorResponse("FATAL", "3D000", `la base de données "db" n'existe pas`))
	})

	_, err := loginTo(t, url)
	var failure *serverError
	if !errors.As(err, &failure) || failure.Code != "3D000" {
		t.Fatalf("connect() = %v, want a serverError with SQLSTATE 3D000", err)
	}
}

func TestLoginRefusesWhatAnHonestServerWouldNotSend(t *testing.T) {
	oversized := binary.BigEndian.AppendUint32([]byte{msgParameterStatus}, 1<<30)

	tests := []struct {
		name string
		send []byte
		want error
	}{
		// A length the peer chose, before it has proved anything, must not
		// become an allocation.
		{"a message of a gigabyte", oversized, errLoginProtocol},
		// The connection is handed over as a bare descriptor after
		// ReadyForQuery. Bytes already read past it would be silently lost.
		{"bytes after ReadyForQuery", append(ready(), ready()...), errLoginProtocol},
		{"an authentication method nobody implements", appendAuthentication(nil, 7, nil), errAuthMethod},
		{"SCRAM with channel binding only", appendAuthentication(nil, authSASL, append(terminated("SCRAM-SHA-256-PLUS"), 0)), errAuthMethod},
		{"a SCRAM challenge nobody asked for", appendAuthentication(nil, authSASLContinue, []byte("r=x,s=eA==,i=1")), errLoginProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			url := scriptedServer(t, func(conn net.Conn, _ *bufio.Reader, _ []byte) {
				conn.Write(test.send)
				io.Copy(io.Discard, conn)
			})
			if _, err := loginTo(t, url); !errors.Is(err, test.want) {
				t.Fatalf("connect() = %v, want %v", err, test.want)
			}
		})
	}
}

// TestLoginHonoursRequiredTLS covers the downgrade: a peer that answers "no" to
// the request for encryption. With sslmode=require that has to end the login
// before the startup packet, which names the user and database, is sent.
func TestLoginHonoursRequiredTLS(t *testing.T) {
	refuseTLS := func(sawStartup chan<- bool) string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen() = %v", err)
		}
		t.Cleanup(func() { listener.Close() })

		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()

			var request [8]byte
			if _, err := io.ReadFull(conn, request[:]); err != nil {
				return
			}
			conn.Write([]byte{'N'})

			var length [4]byte
			if _, err := io.ReadFull(conn, length[:]); err != nil {
				sawStartup <- false
				return
			}
			sawStartup <- true
			io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint32(length[:])-4))
			conn.Write(ready())
			io.Copy(io.Discard, conn)
		}()
		return "postgres://app:pw@" + listener.Addr().String() + "/db"
	}

	t.Run("require", func(t *testing.T) {
		sawStartup := make(chan bool, 1)
		_, err := loginTo(t, refuseTLS(sawStartup)+"?sslmode=require")
		if !errors.Is(err, errTLSRefused) {
			t.Fatalf("connect() = %v, want errTLSRefused", err)
		}
		if <-sawStartup {
			t.Error("startup packet was sent to a server that refused required TLS")
		}
	})
	t.Run("prefer falls back", func(t *testing.T) {
		sawStartup := make(chan bool, 1)
		if _, err := loginTo(t, refuseTLS(sawStartup)+"?sslmode=prefer"); err != nil {
			t.Fatalf("connect() = %v", err)
		}
	})
}

// TestLoginIsInterruptedByItsContext covers a server that accepts and then says
// nothing. Without the deadline the dial would hold a pool permit forever.
func TestLoginIsInterruptedByItsContext(t *testing.T) {
	url := scriptedServer(t, func(conn net.Conn, _ *bufio.Reader, _ []byte) {
		io.Copy(io.Discard, conn)
	})
	target, err := parseUpstream(url)
	if err != nil {
		t.Fatalf("parseUpstream() = %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	started := time.Now()
	if _, err := target.connect(ctx); err == nil {
		t.Fatal("connect() succeeded against a silent server")
	}
	if waited := time.Since(started); waited > 3*time.Second {
		t.Errorf("connect() took %v to notice its context had ended", waited)
	}
}
