package main

import (
	"errors"
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
	bufSize      = 1024
)

var values sync.Map

// read(conn)
// 1) command + command - process few commands
// 2) last command is not processed

func commandWorker(workerId int, listener net.Listener) {
	logger := slog.Default().With("worker", workerId)

	send := func(conn net.Conn, msg string) {
		n, err := conn.Write([]byte(msg))
		if n < len(msg) || err != nil {
			logger.Warn("couldn't send", "sent", n, "size", len(msg), "err", err)
		}
	}
	sendGood := func(conn net.Conn, msg string) {
		send(conn, fmt.Sprintf("+%s\r\n", msg))
	}
	sendBulk := func(conn net.Conn, msg string) {
		send(conn, fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg))
	}
	sendEmptyBulk := func(conn net.Conn) {
		send(conn, "$-1\r\n")
	}
	sendBad := func(conn net.Conn, msg string) {
		send(conn, fmt.Sprintf("-%s\r\n", msg))
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

	cmdSet := func(conn net.Conn, args ...string) {
		if len(args) != 2 {
			sendBad(conn, "ERR 'set' command accepts 2 params")
		}
		values.Store(args[0], args[1])
		sendGood(conn, "OK")
	}

	cmdGet := func(conn net.Conn, args ...string) {
		if len(args) != 1 {
			sendBad(conn, "ERR 'get' command accepts 1 param")
		}

		if res, ok := values.Load(args[0]); !ok {
			sendEmptyBulk(conn)
		} else {
			sendBulk(conn, res.(string))
		}

	}

	commands := map[string]func(net.Conn, ...string){
		"echo": cmdEcho,
		"ping": cmdPing,
		"set":  cmdSet,
		"get":  cmdGet,
	}
next_connection:
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Warn("error accepting connection", "err", err)
			continue
		}
		logger.Debug("new connection established")
		defer conn.Close()
		request := make([]byte, 0, bufSize)
		buf := make([]byte, bufSize)
		for n, err := conn.Read(buf); n != 0 || err != io.EOF; n, err = conn.Read(buf) {
			if err != nil {
				logger.Warn("failed read", "err", err)
				continue next_connection
			}
			request = append(request, buf[:n]...)
			logger.Debug("read from connection", "bytes", n)
			for len(request) != 0 {
				parsedCmd, newStart, err := parseCommand(request)
				errNotParsed := &ErrorNotAllParsed{}
				if errors.As(err, &errNotParsed) {
					break
				}
				if err != nil {
					logger.Warn("bad parsing command", "err", err)
					sendBad(conn, fmt.Sprintf("ERR %v", err))
					continue next_connection
				}
				request = request[newStart:]

				if len(parsedCmd) == 0 {
					sendBad(conn, "ERR empty command")
					continue next_connection
				}
				cmd, ok := commands[parsedCmd[0]]
				if !ok {
					sendBad(conn, fmt.Sprintf("ERR unknown command %s", parsedCmd[0]))
					continue next_connection
				}
				cmd(conn, parsedCmd[1:]...)
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
