package main

import (
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	workersCount = 4
	bufSize      = 1024
)

// command:
// name
// action
// answer
//

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

func getRDBSnapshot() []byte {
	emptySnapshot := []byte("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
	rdbSnapshot := make([]byte, 88)
	_, err := base64.StdEncoding.Decode(rdbSnapshot, emptySnapshot)
	if err != nil {
		log.Panic("something wrong with rdb snapshot", err)
	}
	res := []byte("$88\r\n")
	res = append(res, rdbSnapshot...)
	return res
}

func readFromConnection(logger *slog.Logger, commands map[string]Command, conn net.Conn, commandSource CommandSourceType) {
	request := make([]byte, 0, bufSize)
	buf := make([]byte, bufSize)
	for n, err := conn.Read(buf); n != 0 || err != io.EOF; n, err = conn.Read(buf) {
		if err != nil {
			logger.Warn("failed read", "err", err)
			return
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
				return
			}
			request = request[newStart:]

			if len(parsedCmd) == 0 {
				send(conn, respError("ERR empty command"))
				return
			}
			cmd, ok := commands[parsedCmd[0]]
			if !ok {
				send(conn, respError(fmt.Sprintf("ERR unknown command %s", parsedCmd[0])))
				return
			}
			if err = cmd.Call(conn, commandSource, parsedCmd[1:]...); err != nil {
				logger.Warn("error perform command", "cmd", cmd, "err", err)
			}
		}
	}
}

// TODO: move logger to context
func commandWorker(commandSource CommandSourceType, workerId int, listener net.Listener, replicasManager *ReplicasManager, commands map[string]Command) {
	logger := slog.Default().With("worker", workerId)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Warn("error accepting connection", "err", err)
			continue
		}
		logger.Debug("new connection established")
		// TODO: close connection?
		readFromConnection(logger, commands, conn, commandSource)
	}
}

func parseAddress(hostPort string) string {
	return strings.Join(strings.Split(hostPort, " "), ":")
}

func main() {
	var (
		level           slog.Level
		replicasManager *ReplicasManager
		commandSource   CommandSourceType
	)
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
		commandSource = UserToReplica
		redisInfo = NewRedisInfo("slave")
		address := parseAddress(*replicaOf)
		redisClient, err = NewRedisClient(address, *port)
		if err != nil {
			log.Panic(err)
		}
		slog.Info("connected to redis server", "address", address)
	} else {
		commandSource = UserToMaster
		redisInfo = NewRedisInfo("master")
		replicasManager = NewReplicasManager()
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d", *port)
		os.Exit(1)
	}
	slog.Debug("redis is listening", "port", *port)

	commands := map[string]Command{
		"echo":     CommandEcho{},
		"ping":     CommandPing{},
		"set":      CommandSet{replicasManager: replicasManager, values: &values},
		"get":      CommandGet{values: &values},
		"info":     CommandInfo{redisInfo: &redisInfo},
		"replconf": CommandReplConf{},
		"psync":    CommandPsync{replicasManager},
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			commandWorker(commandSource, i, listener, replicasManager, commands)
		}()
	}

	if replicasManager != nil {
		go replicasManager.NotifyReplicas()
		// TODO: graceful replica notifiers
	} else {
		go redisClient.Listen(commands)
	}
	wg.Wait()
}
