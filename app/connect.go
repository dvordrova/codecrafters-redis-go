package main

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"
)

type RedisConnect struct {
	PrevReadBytes int
	ReadBytes     int
	Conn          net.Conn
	IsBorrowed    bool
	reader        *bufio.Reader
	writer        *bufio.Writer
	readTimeout   time.Duration
	writeTimeout  time.Duration
}

func NewRedisConnect(conn net.Conn) *RedisConnect {
	// Use sync.Pool for conn
	return &RedisConnect{
		Conn:         conn,
		IsBorrowed:   false,
		reader:       bufio.NewReader(conn),
		writer:       bufio.NewWriter(conn),
		readTimeout:  1 * time.Second,
		writeTimeout: 1 * time.Second,
	}
}

func (rc *RedisConnect) ReadLine() (string, error) {
	// rc.Conn.SetReadDeadline(time.Now().Add(rc.readTimeout))
	var text []byte
	for {
		appendText, isPrefix, err := rc.reader.ReadLine()
		if err != nil {
			return "", fmt.Errorf("can't read line: %w", err)
		}
		text = append(text, appendText...)
		if !isPrefix {
			break
		}
	}
	rc.ReadBytes += len(text) + 2
	return string(text), nil
}

func (rc *RedisConnect) ReadFull(buf []byte) error {
	n, err := io.ReadFull(rc.reader, buf)
	rc.ReadBytes += n
	return err
}

func (rc *RedisConnect) SendCommand(cmd string, args ...string) error {
	// rc.Conn.SetWriteDeadline(time.Now().Add(rc.writeTimeout))
	_, err := rc.writer.WriteString(respCommand(cmd, args...))
	if err != nil {
		return fmt.Errorf("sending %s command err: %w", cmd, err)
	}
	err = rc.writer.Flush()
	if err != nil {
		return fmt.Errorf("sending %s command flush err: %w", cmd, err)
	}
	return nil
}

func (rc *RedisConnect) Send(msg string) error {
	// rc.Conn.SetWriteDeadline(time.Now().Add(rc.writeTimeout))
	_, err := rc.writer.WriteString(msg)
	if err != nil {
		return fmt.Errorf("sending msg err: %w", err)
	}
	err = rc.writer.Flush()
	if err != nil {
		return fmt.Errorf("sending msg flush err: %w", err)
	}
	return nil
}

func (rc *RedisConnect) ReadRDBSnapshot() error {
	// rc.Conn.SetReadDeadline(time.Now().Add(rc.readTimeout))
	text, err := rc.ReadLine()
	if err != nil {
		return fmt.Errorf("reading length of RDB snapshot failed: %w", err)
	}
	n, err := strconv.Atoi(text[1:])
	if err != nil {
		return fmt.Errorf("reading length of RDB snapshot failed: %w", err)
	}
	buf := make([]byte, n)
	err = rc.ReadFull(buf)

	if err != nil {
		return fmt.Errorf("reading payload of RDB snapshot failed: %w", err)
	}
	slog.Debug("read from connection", "buf", buf)

	// TODO: return snapshot to make something with it
	return nil
}

func (rc *RedisConnect) RememberPreviousBytes() {
	rc.PrevReadBytes = rc.ReadBytes
}

func (rc *RedisConnect) ReadCommand() ([]string, error) {
	text, err := rc.ReadLine()
	if len(text) == 0 || err != nil || text[0] != '*' {
		return nil, fmt.Errorf("expecting RESP array as a comand: %w", err)
	}
	arrayLength, err := strconv.Atoi(text[1:])
	if err != nil {
		return nil, fmt.Errorf("can't parse array length: %w", err)
	}
	res := make([]string, 0, arrayLength)
	for i := 0; i < arrayLength; i++ {
		text, err := rc.ReadLine()
		if len(text) == 0 || err != nil || text[0] != '$' {
			return nil, fmt.Errorf("expecting array of bulk strings: %w", err)
		}
		bufLen, err := strconv.Atoi(text[1:])
		if err != nil {
			return nil, fmt.Errorf("can't parse bulk string length: %w", err)
		}
		buf := make([]byte, bufLen+2)
		err = rc.ReadFull(buf)
		if err != nil {
			return nil, fmt.Errorf("can't read bulk string payload: %w", err)
		}
		if buf[bufLen] != '\r' || buf[bufLen+1] != '\n' {
			return nil, fmt.Errorf("can't read bulk string payload: %w", err)
		}
		res = append(res, string(buf[:bufLen]))
	}

	return res, nil
}
