package main

import (
	"fmt"
	"net"
)

type RedisClient struct {
	conn net.Conn
}

func NewRedisClient(address string) (*RedisClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("new redis client connect to %s: %w", address, err)
	}
	client := RedisClient{conn: conn}
	if err = client.DoHandShake(); err != nil {
		return nil, fmt.Errorf("redis handshake to %s failed: %w", address, err)
	}
	return &client, nil
}

func (client *RedisClient) DoHandShake() error {
	pingCmd := []byte(respCommand("PING"))
	n, err := client.conn.Write(pingCmd)
	if n < len(pingCmd) || err != nil {
		return fmt.Errorf("sending %w", err)
	}
	answ := make([]byte, 7)
	n, err = client.conn.Read(answ)
	if n != 7 || err != nil || string(answ) != "+PONG\r\n" {
		return fmt.Errorf("expected PONG answer, but %w", err)
	}
	return nil
}
