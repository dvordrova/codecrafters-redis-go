package main

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
)

type RedisClient struct {
	conn *RedisConnect
}

func NewRedisClient(address string, myPort int) (*RedisClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("new redis client connect to %s: %w", address, err)
	}
	client := RedisClient{conn: NewRedisConnect(conn)}
	if err = client.doHandShake(myPort); err != nil {
		// error handling?
		conn.Close()
		return nil, fmt.Errorf("redis handshake to %s failed: %w", address, err)
	}
	return &client, nil
}

func (client *RedisClient) doHandShake(myPort int) error {
	err := client.conn.SendCommand("PING")
	if err != nil {
		return fmt.Errorf("ping command failed: %w", err)
	}

	result, err := client.conn.ReadLine()
	if result != "+PONG" || err != nil {
		return fmt.Errorf("ping command result=%s, err: %w", result, err)
	}

	err = client.conn.SendCommand("REPLCONF", "listening-port", strconv.Itoa(myPort))
	if err != nil {
		return fmt.Errorf("replconf listening-port command failed: %w", err)
	}
	result, err = client.conn.ReadLine()
	if result != "+OK" || err != nil {
		return fmt.Errorf("replconf listening-port command result=%s, err: %w", result, err)
	}

	err = client.conn.SendCommand("REPLCONF", "capa", "psync2")
	if err != nil {
		return fmt.Errorf("replconf capa command failed: %w", err)
	}
	result, err = client.conn.ReadLine()
	if result != "+OK" || err != nil {
		return fmt.Errorf("replconf capa command result=%s, err: %w", result, err)
	}

	err = client.conn.SendCommand("PSYNC", "?", "-1")
	_, err = client.conn.ReadLine()
	// TODO: not ignore psync answer
	if err != nil {
		return fmt.Errorf("psync command failed: %w", err)
	}
	slog.Debug("PSYNC", "result", result)
	err = client.conn.ReadRDBSnapshot()
	if err != nil {
		return fmt.Errorf("reading rdb snapshot failed: %w", err)
	}

	return nil
}

func (client *RedisClient) Listen(commands map[string]Command) {
	logger := slog.Default().With("worker", "replica-listener")
	for {
		readFromConnection(logger, commands, client.conn, MasterToReplica)
	}
}
