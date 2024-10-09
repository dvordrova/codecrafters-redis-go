package main

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FIXME: command performed -> must replication, even if it is error for reply to client

type CommandSourceType int64

const (
	UserToMaster CommandSourceType = iota
	UserToReplica
	MasterToReplica
)

func send(conn net.Conn, msg string) error {
	slog.Debug("sending", "msg", msg)
	n, err := conn.Write([]byte(msg))
	if n < len(msg) || err != nil {
		return fmt.Errorf("couldn't send %d bytes, sent %d bytes: %w", n, len(msg), err)
	}
	return nil
}

type Command interface {
	Call(net.Conn, CommandSourceType, ...string) error
}

type CommandPing struct {
}

func (cmdPing CommandPing) Call(conn net.Conn, _ CommandSourceType, args ...string) error {
	return send(conn, respString("PONG"))
}

type CommandEcho struct {
}

func (cmdEcho CommandEcho) Call(conn net.Conn, _ CommandSourceType, args ...string) error {
	if len(args) != 1 {
		send(conn, respError("ERR 'echo' command accepts 1 param"))
		return fmt.Errorf("ERR 'echo' command accepts 1 param")
	}
	return send(conn, respString(args[0]))
}

type CommandInfo struct {
	redisInfo *RedisInfo
}

func (cmdInfo CommandInfo) Call(conn net.Conn, _ CommandSourceType, args ...string) error {
	if len(args) != 1 {
		send(conn, respError("ERR 'info' command accepts 1 param"))
		return fmt.Errorf("ERR 'info' command accepts 1 param")
	}
	return send(conn, respBulkString(cmdInfo.redisInfo.String()))
}

type CommandSet struct {
	replicasManager *ReplicasManager
	values          *sync.Map
}

func (cmdSet CommandSet) Call(conn net.Conn, commandSource CommandSourceType, args ...string) error {
	if commandSource == UserToReplica {
		send(conn, respError("READONLY You can't write against a read only replica."))
		return fmt.Errorf("READONLY You can't write against a read only replica.")
	}
	usageError := fmt.Errorf("ERR 'set' usage: set <key> <value> [PX <time_ms>]")
	if len(args) != 2 && len(args) != 4 {
		send(conn, respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
		return usageError
	}
	if len(args) == 2 {
		cmdSet.values.Store(args[0], args[1])
		if cmdSet.replicasManager != nil {
			go cmdSet.replicasManager.LogCommand("set", args...)
		}
		if commandSource != MasterToReplica {
			return send(conn, respString("OK"))
		}
		return nil
	}
	if strings.ToLower(args[2]) != "px" {
		send(conn, respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
		return usageError
	}
	ms, err := strconv.Atoi(args[3])
	if err != nil || ms < 0 {
		send(conn, respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
		return usageError
	}
	cmdSet.values.Store(args[0], ValueWithExpiration{
		Value:  args[1],
		Expire: time.Now().Add(time.Duration(ms) * time.Millisecond),
	})
	if cmdSet.replicasManager != nil {
		go cmdSet.replicasManager.LogCommand("set", args...)
		if commandSource != MasterToReplica {
			return send(conn, respString("OK"))
		}
		return nil
	}
	return send(conn, respString("OK"))
}

type CommandGet struct {
	values *sync.Map
}

func (cmdGet CommandGet) Call(conn net.Conn, _ CommandSourceType, args ...string) error {
	usageError := fmt.Errorf("ERR 'get' command accepts 1 param")

	if len(args) != 1 {
		send(conn, respError("ERR 'get' command accepts 1 param"))
		return usageError
	}

	key := args[0]
	value, ok := cmdGet.values.Load(key)
	if !ok {
		return send(conn, respBulkString())
	}
	switch r := value.(type) {
	case ValueWithExpiration:
		if time.Now().Before(r.Expire) {
			return send(conn, respBulkString(r.Value))
		}
		cmdGet.values.CompareAndDelete(key, value)
		return send(conn, respBulkString())
	case string:
		return send(conn, respBulkString(r))
	default:
		return fmt.Errorf("something strange saved in map key=%s", key)
	}
}

type CommandReplConf struct{}

func (cmdReplConf CommandReplConf) Call(conn net.Conn, commandSource CommandSourceType, args ...string) error {
	slog.Debug("REPLCONF", "args", args, "commandSource", commandSource)
	if commandSource == MasterToReplica {
		if strings.ToLower(args[0]) == "getack" {
			return send(conn, respCommand("REPLCONF", "ACK", "0"))
		}
	}
	return send(conn, respString("OK"))
}

type CommandPsync struct {
	replicasManager *ReplicasManager
}

func (cmdPsync CommandPsync) Call(conn net.Conn, _ CommandSourceType, args ...string) error {
	err := send(conn, respString(fmt.Sprintf("FULLRESYNC %s 0", redisInfo.GetMasterReplId())))
	if err != nil {
		return fmt.Errorf("can't return FULLRESYNC answer: %w", err)
	}
	err = send(conn, string(getRDBSnapshot()))
	if err != nil {
		return fmt.Errorf("can't send RDB snapshot: %w", err)
	}
	cmdPsync.replicasManager.RegisterReplica(conn)
	return nil
}
