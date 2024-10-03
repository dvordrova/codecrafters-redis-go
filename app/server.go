package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	workersCount = 4
	bufSize      = 1024
)

var (
	values   sync.Map
	port     = flag.Int("port", 6379, "port")
	logLevel = flag.String("loglevel", "DEBUG", "log level")
)

type ValueWithExpiration struct {
	Value  string
	Expire time.Time
}

func commandWorker(workerId int, listener net.Listener) {
	logger := slog.Default().With("worker", workerId)

	send := func(conn net.Conn, msg string) {
		logger.Debug("sending", "msg", msg)
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
		if len(args) != 2 && len(args) != 4 {
			sendBad(conn, "ERR 'set' usage: set <key> <value> [PX <time_ms>]")
			return
		}
		if len(args) == 2 {
			values.Store(args[0], args[1])
			sendGood(conn, "OK")
			return
		}
		if strings.ToLower(args[2]) != "px" {
			sendBad(conn, "ERR 'set' usage: set <key> <value> [PX <time_ms>]")
			return
		}
		ms, err := strconv.Atoi(args[3])
		if err != nil || ms < 0 {
			sendBad(conn, "ERR 'set' usage: set <key> <value> [PX <time_ms>]")
			return
		}
		values.Store(args[0], ValueWithExpiration{
			Value:  args[1],
			Expire: time.Now().Add(time.Duration(ms) * time.Millisecond),
		})
		sendGood(conn, "OK")
	}

	cmdGet := func(conn net.Conn, args ...string) {
		if len(args) != 1 {
			sendBad(conn, "ERR 'get' command accepts 1 param")
		}

		key := args[0]
		value, ok := values.Load(key)
		if !ok {
			sendEmptyBulk(conn)
			return
		}
		switch r := value.(type) {
		case ValueWithExpiration:
			if time.Now().Before(r.Expire) {
				sendBulk(conn, r.Value)
			} else {
				values.CompareAndDelete(key, value)
				sendEmptyBulk(conn)
			}
		case string:
			sendBulk(conn, r)
		default:
			logger.Error("something strange saved in map %s", "key", key)
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
	var level slog.Level
	flag.Parse()
	err := level.UnmarshalText([]byte(*logLevel))
	if err != nil {
		log.Panicf("error parsing log-level: %v", err)
	}
	logger := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(logger))

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d", *port)
		os.Exit(1)
	}
	slog.Debug("redis is listening", "port", *port)

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
