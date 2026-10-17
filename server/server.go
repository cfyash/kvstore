package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cfyash/kvstore"
)

const (
	SET         string        = "SET"
	GET         string        = "GET"
	DEL         string        = "DEL"
	PING        string        = "PING"
	idleTimeout time.Duration = 30 * time.Second
)

type Server struct {
	db          *kvstore.DB
	shutdown    atomic.Bool
	mu          sync.Mutex
	connections map[net.Conn]struct{}
}

func NewServer(db *kvstore.DB) *Server {
	return &Server{
		db:          db,
		connections: make(map[net.Conn]struct{}),
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	clientAddr := conn.RemoteAddr().String()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	defer func() {
		err := writer.Flush()
		if err != nil {
			log.Printf("error while flushing response %s", err)
		}
	}()
	for {
		conn.SetReadDeadline(time.Now().Add(idleTimeout))
		if s.shutdown.Load() {
			return
		}
		args, err := readCommand(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}
			if errors.Is(err, bufio.ErrBufferFull) || errors.Is(err, errProtocol) || errors.Is(err, errFormat) {
				writer.Write(formatError(err))
				return
			}
			log.Println("closing conenction ", clientAddr)
			return
		}
		data, err := s.dispatch(args)
		if err != nil {
			log.Printf("error while executing db command %s\n", err.Error())
			_, err := writer.Write(formatError(err))
			if err != nil {
				log.Printf("error while sending response %s", err)
			}
		} else {
			_, err := writer.Write(data)
			if err != nil {
				log.Printf("error while sending response %s", err)
			}
		}
		if reader.Buffered() == 0 {
			err = writer.Flush()
			if err != nil {
				log.Printf("error while flushing response %s", err)
			}
		}
	}
}

func (s *Server) dispatch(args [][]byte) ([]byte, error) {
	if len(args) == 0 {
		return nil, errProtocol
	}
	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case SET:
		{
			if len(args) != 3 {
				return nil, fmt.Errorf("%w: invalid number of arguments, expects 2", errSyntax)
			}
			err := s.db.Write(string(args[1]), args[2])
			if err == nil {
				return formatOK(), nil
			}
			return nil, err
		}
	case GET:
		{
			if len(args) != 2 {
				return nil, fmt.Errorf("%w: invalid number of arguments, expects 1", errSyntax)
			}
			val, err := s.db.Read(string(args[1]))
			if err != nil {
				if errors.Is(err, kvstore.ErrNotFound) {
					return formatNil(), nil
				}
				return nil, err
			}
			return formatData(val), nil
		}
	case DEL:
		{
			if len(args) != 2 {
				return nil, fmt.Errorf("%w: invalid number of arguments, expects 1", errSyntax)
			}
			ok, err := s.db.Delete(string(args[1]))
			if err != nil {
				if errors.Is(err, kvstore.ErrNotFound) {
					return formatBool(ok), nil
				} else {
					log.Printf("Error encountered while deleting record %s\n", err)
					return formatBool(ok), err
				}
			}
			return formatBool(ok), nil
		}
	case PING:
		{
			if len(args) != 1 {
				return nil, fmt.Errorf("%w: invalid number of arguments, expects 0", errSyntax)
			}
			return formatPONG(), nil
		}
	default:
		{
			return nil, fmt.Errorf("%w: unknown command %s", errSyntax, string(args[0]))
		}
	}
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	go func() {
		<-ctx.Done()
		s.shutdown.Store(true)
		listener.Close()
		s.mu.Lock()
		for conn, _ := range s.connections {
			conn.SetReadDeadline(time.Now())
		}
		s.mu.Unlock()
	}()
	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			wg.Wait()
			return nil
		}
		if err != nil {
			log.Printf("error in accepting new connection %v\n", err)
			continue
		}
		s.mu.Lock()
		s.connections[conn] = struct{}{}
		s.mu.Unlock()
		wg.Go(func() {
			s.handleConn(conn)
			s.mu.Lock()
			delete(s.connections, conn)
			s.mu.Unlock()
		})
	}
}
