package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"unsafe"
)

const (
	port         = 6379
	pingRequest  = "*1\r\n$4\r\nPING\r\n"
	pingResponse = "+PONG\r\n"
	bufSize      = 1000
)

func commandWorker(conn net.Conn) error {
	buff := make([]byte, bufSize)
	for n, err := conn.Read(buff); n != 0 && err != io.EOF; n, err = conn.Read(buff) {
		if err != nil {
			return fmt.Errorf("failed to read command: %v\n", err)
		}
		cmd := buff[:n]
		slog.Debug("command reader read", "bytes", n, "cmd", cmd)

		if bytes.Equal(cmd, unsafe.Slice(unsafe.StringData(pingRequest), len(pingRequest))) {
			n, err := conn.Write(unsafe.Slice(unsafe.StringData(pingResponse), len(pingResponse)))
			if err != nil {
				return fmt.Errorf("failed to write command response: %w", err)
			}
			if n != len(pingResponse) {
				return fmt.Errorf("writing ping response resulted in %d bytes, but expected %d", n, len(pingResponse))
			}
		}
	}
	// TODO: read all, set timeout
	return nil
}

func main() {
	logger := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(logger))
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d", port)
		os.Exit(1)
	}

	for {
		err := func() error {
			conn, err := l.Accept()
			if err != nil {
				return fmt.Errorf("error accepting connection: %w\n", err)
			}
			slog.Debug("new connection established")
			defer conn.Close()
			return commandWorker(conn)
		}()
		if err != nil {
			slog.Warn("connection processing error", "err", err)
		}
	}
}
