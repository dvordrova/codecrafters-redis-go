package main

import (
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"sync"
)

const (
	workersCount = 4
	port         = 6379
	bufSize      = 1000
)

func commandWorker(workerId int, listener net.Listener) {
	logger := slog.Default().With("worker", workerId)

	sendGood := func(conn net.Conn, msg string) {
		n, err := conn.Write([]byte(fmt.Sprintf("+%s\r\n", msg)))
		if n < len(msg) || err != nil {
			logger.Warn("couldn't send", "sent", n, "size", len(msg), "err", err)
		}
	}
	sendBad := func(conn net.Conn, msg string) {
		n, err := conn.Write([]byte(fmt.Sprintf("-%s\r\n", msg)))
		if n < len(msg) || err != nil {
			logger.Warn("couldn't send", "sent", n, "size", len(msg), "err", err)
		}
	}

	cmdPing := func(conn net.Conn, args ...string) {
		sendGood(conn, "PONG")
	}

	cmdEcho := func(conn net.Conn, args ...string) {
		if len(args) != 1 {
			sendBad(conn, "ERR 'echo' command accepts 1 param")
		} else {
			sendGood(conn, args[0])
		}
	}

	commands := map[string]func(net.Conn, ...string){
		"echo": cmdEcho,
		"ping": cmdPing,
	}
new_connection:
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Panicf("error accepting connection: %v\n", err)
		}
		logger.Debug("new connection established")
		defer conn.Close()
		request := make([]byte, 0, bufSize)
		buff := make([]byte, bufSize)
		for n, err := conn.Read(buff); n != 0 && err != io.EOF; n, err = conn.Read(buff) {
			if err != nil {
				logger.Warn("failed to read command: %v\n", err)
				continue new_connection
			}
			logger.Debug("command reader read", "bytes", n, "cmd", buff[:n])
			request = append(request, buff[:n]...)
			parsedCmd, err := parseCommand(request)
			if err != nil {
				logger.Warn("bad parsing command", "err", err)
				sendBad(conn, fmt.Sprintf("ERR %v", err))
			}
			if len(parsedCmd) == 0 {
				sendBad(conn, "ERR empty command")
				continue
			}
			cmd, ok := commands[parsedCmd[0]]
			if !ok {
				sendBad(conn, fmt.Sprintf("ERR unknown command %s", parsedCmd[0]))
				continue
			}
			cmd(conn, parsedCmd[1:]...)
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
