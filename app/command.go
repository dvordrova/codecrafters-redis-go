package main

import (
	"fmt"
	"log/slog"
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

type Command interface {
	Call(*RedisConnect, CommandSourceType, ...string) error
}

type CommandPing struct {
}

func (cmdPing CommandPing) Call(conn *RedisConnect, commandSource CommandSourceType, args ...string) error {
	if commandSource == MasterToReplica {
		return nil
	}
	return conn.Send(respString("PONG"))
}

type CommandEcho struct {
}

func (cmdEcho CommandEcho) Call(conn *RedisConnect, _ CommandSourceType, args ...string) error {
	if len(args) != 1 {
		conn.Send(respError("ERR 'echo' command accepts 1 param"))
		return fmt.Errorf("ERR 'echo' command accepts 1 param")
	}
	return conn.Send(respString(args[0]))
}

type CommandInfo struct {
	redisInfo *RedisInfo
}

func (cmdInfo CommandInfo) Call(conn *RedisConnect, _ CommandSourceType, args ...string) error {
	if len(args) != 1 {
		conn.Send(respError("ERR 'info' command accepts 1 param"))
		return fmt.Errorf("ERR 'info' command accepts 1 param")
	}
	return conn.Send(respBulkString(cmdInfo.redisInfo.String()))
}

type CommandSet struct {
	replicasManager *ReplicasManager
	values          *sync.Map
}

func (cmdSet CommandSet) Call(conn *RedisConnect, commandSource CommandSourceType, args ...string) error {
	if commandSource == UserToReplica {
		conn.Send(respError("READONLY You can't write against a read only replica."))
		return fmt.Errorf("READONLY You can't write against a read only replica.")
	}
	usageError := fmt.Errorf("ERR 'set' usage: set <key> <value> [PX <time_ms>]")
	if len(args) != 2 && len(args) != 4 {
		conn.Send(respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
		return usageError
	}
	if len(args) == 2 {
		cmdSet.values.Store(args[0], args[1])
		if cmdSet.replicasManager != nil {
			go cmdSet.replicasManager.LogCommand("set", args...)
		}
		if commandSource != MasterToReplica {
			return conn.Send(respString("OK"))
		}
		return nil
	}
	if strings.ToLower(args[2]) != "px" {
		conn.Send(respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
		return usageError
	}
	ms, err := strconv.Atoi(args[3])
	if err != nil || ms < 0 {
		conn.Send(respError("ERR 'set' usage: set <key> <value> [PX <time_ms>]"))
		return usageError
	}
	cmdSet.values.Store(args[0], ValueWithExpiration{
		Value:  args[1],
		Expire: time.Now().Add(time.Duration(ms) * time.Millisecond),
	})
	if cmdSet.replicasManager != nil {
		go cmdSet.replicasManager.LogCommand("set", args...)
		if commandSource != MasterToReplica {
			return conn.Send(respString("OK"))
		}
		return nil
	}
	return conn.Send(respString("OK"))
}

type CommandGet struct {
	values *sync.Map
}

func (cmdGet CommandGet) Call(conn *RedisConnect, _ CommandSourceType, args ...string) error {
	usageError := fmt.Errorf("ERR 'get' command accepts 1 param")

	if len(args) != 1 {
		conn.Send(respError("ERR 'get' command accepts 1 param"))
		return usageError
	}

	key := args[0]
	value, ok := cmdGet.values.Load(key)
	if !ok {
		return conn.Send(respBulkString())
	}
	switch r := value.(type) {
	case ValueWithExpiration:
		if time.Now().Before(r.Expire) {
			return conn.Send(respBulkString(r.Value))
		}
		cmdGet.values.CompareAndDelete(key, value)
		return conn.Send(respBulkString())
	case string:
		return conn.Send(respBulkString(r))
	default:
		return fmt.Errorf("something strange saved in map key=%s", key)
	}
}

type CommandReplConf struct{}

func (cmdReplConf CommandReplConf) Call(conn *RedisConnect, commandSource CommandSourceType, args ...string) error {
	slog.Debug("REPLCONF", "args", args, "commandSource", commandSource)
	if commandSource == MasterToReplica {
		if strings.ToLower(args[0]) == "getack" {
			return conn.Send(respCommand("REPLCONF", "ACK", strconv.Itoa(conn.PrevReadBytes)))
		}
	}
	return conn.Send(respString("OK"))
}

type CommandPsync struct {
	replicasManager *ReplicasManager
}

func (cmdPsync CommandPsync) Call(conn *RedisConnect, _ CommandSourceType, args ...string) error {
	err := conn.Send(respString(fmt.Sprintf("FULLRESYNC %s 0", redisInfo.GetMasterReplId())))
	if err != nil {
		return fmt.Errorf("can't return FULLRESYNC answer: %w", err)
	}
	err = conn.Send(string(getRDBSnapshot()))
	if err != nil {
		return fmt.Errorf("can't send RDB snapshot: %w", err)
	}
	cmdPsync.replicasManager.RegisterReplica(conn)
	return nil
}

type CommandWait struct {
	replicasManager *ReplicasManager
}

func (cmdWait CommandWait) Call(conn *RedisConnect, _ CommandSourceType, args ...string) error {
	return conn.Send(respInt(cmdWait.replicasManager.GetReplicasCount()))
}
