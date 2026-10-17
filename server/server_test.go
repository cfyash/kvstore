package server

// Black-box tests for the RESP server: each test starts a real server on a
// random localhost port and talks to it over TCP, exactly like redis-cli would.
//
//   go test ./server/ -v          run everything
//   go test ./server/ -race -v    same, with the data race detector
//   go test ./server/ -run Pipeline -v    one test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cfyash/kvstore"
)

// startServer opens a fresh DB in a temp dir and serves it on 127.0.0.1:<random>.
func startServer(t *testing.T) string {
	t.Helper()
	db, err := kvstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	go NewServer(db).Serve(ctx, l)
	return l.Addr().String()
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &client{t, c, bufio.NewReader(c)}
}

// resp encodes a command the way real clients do: an array of bulk strings.
func resp(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.String()
}

func (c *client) send(raw string) {
	c.t.Helper()
	if _, err := c.conn.Write([]byte(raw)); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// reply reads exactly one RESP reply and returns it raw, e.g. "+OK\r\n" or "$3\r\nbar\r\n".
// On a read error it returns "<error text>" so tests can check for closed connections.
func (c *client) reply() string {
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "<" + err.Error() + ">"
	}
	if line[0] == '$' && line != "$-1\r\n" {
		var n int
		fmt.Sscanf(line, "$%d", &n)
		body := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, body); err != nil {
			return "<" + err.Error() + ">"
		}
		return line + string(body)
	}
	return line
}

func (c *client) expect(want string) {
	c.t.Helper()
	if got := c.reply(); got != want {
		c.t.Errorf("want %q, got %q", want, got)
	}
}

func bulk(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }

func TestCommands(t *testing.T) {
	c := dial(t, startServer(t))
	steps := []struct {
		args []string
		want string
	}{
		{[]string{"PING"}, "+PONG\r\n"},
		{[]string{"ping"}, "+PONG\r\n"},
		{[]string{"SET", "foo", "bar"}, "+OK\r\n"},
		{[]string{"GET", "foo"}, bulk("bar")},
		{[]string{"get", "missing"}, "$-1\r\n"},
		{[]string{"SET", "foo", "baz"}, "+OK\r\n"},
		{[]string{"GET", "foo"}, bulk("baz")},
		{[]string{"DEL", "foo"}, ":1\r\n"},
		{[]string{"DEL", "foo"}, ":0\r\n"},
		{[]string{"GET", "foo"}, "$-1\r\n"},
		{[]string{"SET", "bin", "a\r\nb"}, "+OK\r\n"}, // binary-safe: CRLF inside a value
		{[]string{"GET", "bin"}, bulk("a\r\nb")},
	}
	for _, s := range steps {
		c.send(resp(s.args...))
		if got := c.reply(); got != s.want {
			t.Errorf("%v: want %q, got %q", s.args, s.want, got)
		}
	}
}

func TestValueSizeLimit(t *testing.T) {
	c := dial(t, startServer(t))
	maxVal := make([]byte, maxBulkLen)
	moreThanMaxVal := make([]byte, maxBulkLen+1)
	cmds := [][]string{
		{"SET", "big", string(maxVal)},
		{"GET", "big"},
		{"SET", "big2", string(moreThanMaxVal)},
	}
	c.send(resp(cmds[0]...))
	if got := c.reply(); !strings.HasPrefix(got, "+OK") {
		t.Errorf("%q: want +OK reply", cmds[0])
	}
	c.send(resp(cmds[1]...))
	if got := c.reply(); bulk(string(maxVal)) != got {
		t.Errorf("%q: response at maxLen not equals to stored", cmds[1])
	}
	c.send("*3\r\n$3\r\nSET\r\n$4\r\nbig2\r\n$1048577\r\n")
	if got := c.reply(); !strings.HasPrefix(got, "-ERR format error: command rejected") {
		t.Errorf("%q: want -ERR reply, got %q", cmds[2], got)
	}
}

// Command errors: reply -ERR and KEEP the connection usable.
func TestCommandErrorsKeepConnection(t *testing.T) {
	c := dial(t, startServer(t))
	for _, args := range [][]string{
		{"HELLO"},          // unknown command
		{"SET", "onlykey"}, // wrong arg count
		{"GET"},
		{"PING", "extra"},
		{"SET", "k", ""}, // empty value: DB rejects it, but framing is fine
	} {
		c.send(resp(args...))
		if got := c.reply(); !strings.HasPrefix(got, "-ERR") {
			t.Errorf("%q: want -ERR reply, got %q", args, got)
		}
		c.send(resp("PING"))
		if got := c.reply(); got != "+PONG\r\n" {
			t.Errorf("after %q: connection should still work, PING got %q", args, got)
		}
	}
}

// Protocol errors: reply -ERR, then CLOSE. The server itself must survive.
func TestProtocolErrorsCloseConnection(t *testing.T) {
	addr := startServer(t)
	cases := map[string]string{
		"no array":             "GET foo\r\n",
		"zero elements":        "*0\r\n",
		"negative count":       "*-1\r\n",
		"huge count":           "*2000000000\r\n",
		"count not a number":   "*abc\r\n",
		"element not bulk":     "*1\r\n:4\r\n",
		"negative bulk len":    "*1\r\n$-5\r\n",
		"huge bulk len":        "*1\r\n$4000000000\r\n",
		"bad bytes after data": "*1\r\n$4\r\nPINGxx",
		"header missing CR":    "*1\n",
		"line too long":        strings.Repeat("A", 5000),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			c := dial(t, addr)
			c.send(payload)
			if got := c.reply(); !strings.HasPrefix(got, "-ERR") {
				t.Errorf("want -ERR before close, got %q", got)
			}
			if got := c.reply(); !strings.HasPrefix(got, "<") {
				t.Errorf("want connection closed after protocol error, got %q", got)
			}
			// the server process must still be alive for everyone else
			d := dial(t, addr)
			d.send(resp("PING"))
			d.expect("+PONG\r\n")
		})
	}
}

// Client hangs up halfway through a command: server closes quietly, stays up.
func TestTornCommand(t *testing.T) {
	addr := startServer(t)
	c := dial(t, addr)
	c.send("*2\r\n$3\r\nGE")
	c.conn.Close()
	d := dial(t, addr)
	d.send(resp("PING"))
	d.expect("+PONG\r\n")
}

// Many commands in one write; replies must come back complete and in order.
func TestPipeline(t *testing.T) {
	c := dial(t, startServer(t))
	const n = 500
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(resp("SET", fmt.Sprint("k", i), fmt.Sprint("v", i)))
	}
	for i := 0; i < n; i++ {
		b.WriteString(resp("GET", fmt.Sprint("k", i)))
	}
	c.send(b.String())
	for i := 0; i < n; i++ {
		c.expect("+OK\r\n")
	}
	for i := 0; i < n; i++ {
		c.expect(bulk(fmt.Sprint("v", i)))
	}
}

// TCP may deliver a command in arbitrary pieces; here, one byte at a time.
func TestSplitAcrossReads(t *testing.T) {
	c := dial(t, startServer(t))
	p := resp("SET", "split", "value") + resp("GET", "split")
	for i := 0; i < len(p); i++ {
		c.send(p[i : i+1])
		time.Sleep(time.Millisecond)
	}
	c.expect("+OK\r\n")
	c.expect(bulk("value"))
}

// Many clients at once, each pipelining SET+GET on its own keys. Run with -race.
func TestConcurrentClients(t *testing.T) {
	addr := startServer(t)
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			c := &client{t, conn, bufio.NewReader(conn)}
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("g%d-%d", g, i%20)
				v := fmt.Sprintf("val-%d-%d", g, i)
				c.conn.Write([]byte(resp("SET", k, v) + resp("GET", k)))
				if r := c.reply(); r != "+OK\r\n" {
					t.Errorf("client %d SET: got %q", g, r)
					return
				}
				if r := c.reply(); r != bulk(v) {
					t.Errorf("client %d GET %s: want %q got %q", g, k, bulk(v), r)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
