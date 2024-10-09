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

type RedisClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	// TODO: rad/write timeouts
}

func NewRedisClient(address string, myPort int) (*RedisClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("new redis client connect to %s: %w", address, err)
	}
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	client := RedisClient{conn: conn, reader: reader, writer: writer}
	if err = client.doHandShake(myPort); err != nil {
		return nil, fmt.Errorf("redis handshake to %s failed: %w", address, err)
	}
	return &client, nil
}

func (client *RedisClient) sendCommand(cmd string, args ...string) (string, error) {
	client.conn.SetDeadline(time.Now().Add(1 * time.Second))
	_, err := client.writer.WriteString(respCommand(cmd, args...))
	if err != nil {
		return "", fmt.Errorf("sending %w", err)
	}
	err = client.writer.Flush()
	if err != nil {
		return "", fmt.Errorf("sending %w", err)
	}
	text, err := client.readLine()
	if err != nil {
		return "", fmt.Errorf("reading asnwer error: %w", err)
	}
	return text, nil
}

func (client *RedisClient) readLine() (string, error) {
	client.conn.SetDeadline(time.Now().Add(1 * time.Second))
	var text []byte
	for {
		appendText, isPrefix, err := client.reader.ReadLine()
		if err != nil {
			return "", fmt.Errorf("can't read line: %w", err)
		}
		text = append(text, appendText...)
		if !isPrefix {
			break
		}
	}
	return string(text), nil
}

func (client *RedisClient) readRDBSnapshot() error {
	client.conn.SetDeadline(time.Now().Add(5 * time.Second))
	text, err := client.readLine()
	if err != nil {
		fmt.Errorf("reading length of RDB snapshot failed: %w", err)
	}
	n, err := strconv.Atoi(text[1:])
	slog.Debug("can be read from buffer", "bytes", client.reader.Buffered())
	if err != nil {
		return fmt.Errorf("reading length of RDB snapshot failed: %w", err)
	}
	buf := make([]byte, n)
	_, err = io.ReadFull(client.reader, buf)

	if err != nil {
		return fmt.Errorf("reading payload of RDB snapshot failed: %w", err)
	}

	// TODO: make something with snapshot
	return nil
}

func (client *RedisClient) doHandShake(myPort int) error {
	result, err := client.sendCommand("PING")
	if err != nil {
		return fmt.Errorf("ping command failed: %w", err)
	}
	if result != "+PONG" {
		return fmt.Errorf("ping command result: %s", result)
	}

	result, err = client.sendCommand("REPLCONF", "listening-port", strconv.Itoa(myPort))
	if err != nil {
		return fmt.Errorf("replconf listening-port command failed: %w", err)
	}
	if result != "+OK" {
		return fmt.Errorf("replconf listening-port command result: %s", result)
	}

	result, err = client.sendCommand("REPLCONF", "capa", "psync2")
	if err != nil {
		return fmt.Errorf("replconf capa command failed: %w", err)
	}
	if result != "+OK" {
		return fmt.Errorf("replconf capa command result: %s", result)
	}

	result, err = client.sendCommand("PSYNC", "?", "-1")
	if err != nil {
		return fmt.Errorf("psync command failed: %w", err)
	}
	slog.Debug("PSYNC", "result", result)
	err = client.readRDBSnapshot()
	if err != nil {
		return fmt.Errorf("reading rdb snapshot failed: %w", err)
	}

	return nil
}

func (client *RedisClient) Listen(commands map[string]Command) {
	logger := slog.Default().With("worker", "replica-listener")
	// FIXME: very strange
	client.conn.SetDeadline(time.Now().Add(1 * time.Hour))
	for {
		readFromConnection(logger, commands, client.conn, MasterToReplica)
	}
}
