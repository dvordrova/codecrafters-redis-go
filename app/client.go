package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"
)

type RedisClient struct {
	conn net.Conn
}

func NewRedisClient(address string, myPort int) (*RedisClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("new redis client connect to %s: %w", address, err)
	}
	client := RedisClient{conn: conn}
	if err = client.doHandShake(myPort); err != nil {
		return nil, fmt.Errorf("redis handshake to %s failed: %w", address, err)
	}
	return &client, nil
}

func (client *RedisClient) sendCommand(cmd string, args ...string) (string, error) {
	client.conn.SetDeadline(time.Now().Add(1 * time.Second))
	writer := bufio.NewWriter(client.conn)
	_, err := writer.WriteString(respCommand(cmd, args...))
	if err != nil {
		return "", fmt.Errorf("sending %w", err)
	}
	err = writer.Flush()
	if err != nil {
		return "", fmt.Errorf("sending %w", err)
	}
	scanner := bufio.NewScanner(bufio.NewReader(client.conn))
	if !scanner.Scan() {
		return "", fmt.Errorf("no asnwer: %w", scanner.Err())
	}
	return scanner.Text(), nil
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

	// TODO
	// if result != "+OK" {
	// 	return fmt.Errorf("psync command result: %s", result)
	// }
	return nil
}
