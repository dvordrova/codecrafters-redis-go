package main

import (
	"fmt"
	"net"
	"strconv"
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

func (client *RedisClient) sendCommand(expectedResponse string, cmd string, args ...string) error {
	cmdBytes := []byte(respCommand(cmd, args...))
	n, err := client.conn.Write(cmdBytes)
	if n < len(cmdBytes) || err != nil {
		return fmt.Errorf("sending %w", err)
	}
	// TODO: read until \r\n and make readable error
	answ := make([]byte, len(expectedResponse)+1)
	n, err = client.conn.Read(answ)
	if err != nil || string(answ[:n]) != expectedResponse {
		return fmt.Errorf("wrong answer, but %w", err)
	}
	return nil
}

func (client *RedisClient) doHandShake(myPort int) error {
	err := client.sendCommand(respString("PONG"), "PING")
	if err != nil {
		return fmt.Errorf("ping command failed: %w", err)
	}
	err = client.sendCommand(respString("OK"), "REPLCONF", "listening-port", strconv.Itoa(myPort))
	if err != nil {
		return fmt.Errorf("replconf command failed: %w", err)
	}
	err = client.sendCommand(respString("OK"), "REPLCONF", "capa", "psync2")
	if err != nil {
		return fmt.Errorf("replconf command failed: %w", err)
	}
	return nil
}
