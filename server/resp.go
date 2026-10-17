package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	errProtocol error = errors.New("protocol error")
	errFormat   error = errors.New("format error")
	errSyntax   error = errors.New("syntax error")
)

const (
	maxBulkLen = 1 << 20
)

func readCommand(reader *bufio.Reader) ([][]byte, error) {
	header, err := reader.ReadSlice('\n')
	if err != nil {
		return [][]byte{}, err
	}
	count, err := parseHeader(header)
	if err != nil {
		return [][]byte{}, err
	}
	args := make([][]byte, 0, count)
	for count > 0 {
		count--
		m, err := reader.ReadSlice('\n')
		if err != nil {
			return [][]byte{}, err
		}
		size, err := parseBodyMarker(m)
		if err != nil {
			return [][]byte{}, err
		}
		buf := make([]byte, size+2)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return [][]byte{}, err
		}
		arg, err := parseBody(buf)
		if err != nil {
			return [][]byte{}, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func parseBody(b []byte) ([]byte, error) {
	err := basicProtocolCheck(b)
	if err != nil {
		return []byte{}, err
	}
	return b[:len(b)-2], nil
}

func parseBodyMarker(m []byte) (int, error) {
	err := basicProtocolCheck(m)
	if err != nil {
		return 0, err
	}
	cmd := m[0]
	if cmd != '$' {
		return 0, fmt.Errorf("%w: expected '$', got %q", errProtocol, m[0])
	}
	count, err := strconv.Atoi(string(m[1 : len(m)-2]))
	if err != nil {
		return 0, fmt.Errorf("%w: expected number, got %q", errProtocol, m[1:len(m)-2])
	}
	if count < 0 || count > maxBulkLen {
		return 0, fmt.Errorf("%w: command rejected, max size %d got %d", errFormat, maxBulkLen, count)
	}
	return count, nil
}

func basicProtocolCheck(b []byte) error {
	if len(b) < 2 || (b)[len(b)-1] != '\n' || (b)[len(b)-2] != '\r' {
		return errProtocol
	}
	return nil
}

func parseHeader(h []byte) (int, error) {
	err := basicProtocolCheck(h)
	if err != nil {
		return 0, err
	}
	cmd := h[0]
	if cmd != '*' {
		return 0, fmt.Errorf("%w: expected '*', got %q", errProtocol, h[0])
	}
	count, err := strconv.Atoi(string(h[1 : len(h)-2]))
	if err != nil {
		return 0, fmt.Errorf("%w: expected number, got %q", errProtocol, h[1:len(h)-2])
	}
	if count < 1 || count > 3 {
		return 0, fmt.Errorf("%w: command rejected, params range (1,3) %d", errFormat, count)
	}
	return count, nil
}

func formatError(err error) []byte {
	return fmt.Appendf(make([]byte, 0), "-ERR %s\r\n", err.Error())
}

func formatBool(f bool) []byte {
	bit := byte('0')
	if f {
		bit = '1'
	}
	return fmt.Appendf(make([]byte, 0), ":%c\r\n", bit)
}

func formatNil() []byte {
	return []byte("$-1\r\n")
}

func formatOK() []byte {
	return []byte("+OK\r\n")
}

func formatPONG() []byte {
	return []byte("+PONG\r\n")
}

func formatData(data []byte) []byte {
	buf := make([]byte, 0)
	buf = fmt.Append(buf, "$"+strconv.Itoa(len(data))+"\r\n")
	buf = append(buf, data...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	return buf
}
