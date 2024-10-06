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
	values      sync.Map
	port        = flag.Int("port", 6379, "port")
	logLevel    = flag.String("loglevel", "DEBUG", "log level")
	replicaOf   = flag.String("replicaof", "", "master replica in format '<MASTER_HOST> <MASTER_PORT>'")
	redisInfo   RedisInfo
	redisClient *RedisClient
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

	cmdPing := func(conn net.Conn, args ...string) {
		send(conn, respString("PONG"))
	}

	cmdEcho := func(conn net.Conn, args ...string) {
		if len(args) != 1 {
			send(conn, respError("ERR 'echo' command accepts 1 param"))
			return
		}
		send(conn, respString(args[0]))
	}

	cmdInfo := func(conn net.Conn, args ...string) {
		if len(args) != 1 {
			send(conn, respError("ERR 'info' command accepts 1 param"))
			return
		}
		send(conn, respString(redisInfo.String()))
	}

	cmdSet := func(conn net.Conn, args ...string) {
		if len(args) != 2 && len(args) != 4 {
			send(conn, respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
			return
		}
		if len(args) == 2 {
			values.Store(args[0], args[1])
			send(conn, respString("OK"))
			return
		}
		if strings.ToLower(args[2]) != "px" {
			send(conn, respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
			return
		}
		ms, err := strconv.Atoi(args[3])
		if err != nil || ms < 0 {
			send(conn, respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
			return
		}
		values.Store(args[0], ValueWithExpiration{
			Value:  args[1],
			Expire: time.Now().Add(time.Duration(ms) * time.Millisecond),
		})
		send(conn, respString("OK"))
	}

	cmdGet := func(conn net.Conn, args ...string) {
		if len(args) != 1 {
			send(conn, respError("ERR 'get' command accepts 1 param"))
		}

		key := args[0]
		value, ok := values.Load(key)
		if !ok {
			send(conn, respBulkString())
			return
		}
		switch r := value.(type) {
		case ValueWithExpiration:
			if time.Now().Before(r.Expire) {
				send(conn, respBulkString(r.Value))
			} else {
				values.CompareAndDelete(key, value)
				send(conn, respBulkString())
			}
		case string:
			send(conn, respBulkString(r))
		default:
			logger.Error("something strange saved in map %s", "key", key)
		}
	}

	cmdReplConf := func(conn net.Conn, args ...string) {
		send(conn, respString("OK"))
	}

	commands := map[string]func(net.Conn, ...string){
		"echo":     cmdEcho,
		"ping":     cmdPing,
		"set":      cmdSet,
		"get":      cmdGet,
		"info":     cmdInfo,
		"replconf": cmdReplConf,
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
					send(conn, respError(fmt.Sprintf("ERR %v", err)))
					continue next_connection
				}
				request = request[newStart:]

				if len(parsedCmd) == 0 {
					send(conn, respError("ERR empty command"))
					continue next_connection
				}
				cmd, ok := commands[parsedCmd[0]]
				if !ok {
					send(conn, respError(fmt.Sprintf("ERR unknown command %s", parsedCmd[0])))
					continue next_connection
				}
				cmd(conn, parsedCmd[1:]...)
			}
		}
	}
}

func parseAddress(hostPort string) string {
	return strings.Join(strings.Split(hostPort, " "), ":")
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

	if *replicaOf != "" {
		// TODO: parse and connect
		redisInfo = NewRedisInfo("slave")
		address := parseAddress(*replicaOf)
		redisClient, err = NewRedisClient(address, *port)
		if err != nil {
			log.Panic(err)
		}
		slog.Info("connected to redis server", "address", address)
	} else {
		redisInfo = NewRedisInfo("master")
	}

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
