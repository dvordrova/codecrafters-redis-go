package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"sync"
	"unsafe"
)

const (
	workersCount = 4
	port         = 6379
	pingRequest  = "*1\r\n$4\r\nPING\r\n"
	pingResponse = "+PONG\r\n"
	bufSize      = 1000
)

func commandWorker(workerId int, listener net.Listener) {
	logger := slog.Default().With("worker", workerId)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Panicf("error accepting connection: %v\n", err)
		}
		logger.Debug("new connection established")
		defer conn.Close()
		buff := make([]byte, bufSize)
		for n, err := conn.Read(buff); n != 0 && err != io.EOF; n, err = conn.Read(buff) {
			if err != nil {
				log.Panicf("failed to read command: %v\n", err)
			}
			cmd := buff[:n]
			logger.Debug("command reader read", "bytes", n, "cmd", cmd)

			if bytes.Equal(cmd, unsafe.Slice(unsafe.StringData(pingRequest), len(pingRequest))) {
				n, err := conn.Write(unsafe.Slice(unsafe.StringData(pingResponse), len(pingResponse)))
				if err != nil {
					log.Panicf("failed to write command response: %v", err)
				}
				if n != len(pingResponse) {
					log.Panicf("writing ping response resulted in %d bytes, but expected %d", n, len(pingResponse))
				}
			}
		}
	}
}

func main() {
	logger := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(logger))
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d", port)
		os.Exit(1)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			commandWorker(i, listener)
		}()
	}
	wg.Wait()
}
