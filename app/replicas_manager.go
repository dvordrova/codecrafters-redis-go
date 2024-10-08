package main

import (
	"log/slog"
	"net"
	"sync"
)

type ReplicasManager struct {
	replicasConn      []net.Conn
	replicasConnMutex sync.RWMutex
	replicasCommands  chan []string
}

func NewReplicasManager() *ReplicasManager {
	return &ReplicasManager{
		replicasCommands: make(chan []string),
	}
}

func (rm *ReplicasManager) RegisterReplica(conn net.Conn) {
	slog.Debug("new replica registered")
	rm.replicasConnMutex.Lock()
	defer rm.replicasConnMutex.Unlock()
	rm.replicasConn = append(rm.replicasConn, conn)
}

func (rm *ReplicasManager) LogCommand(cmd string, args ...string) {
	cmds := make([]string, 0, len(args)+1)
	cmds = append(cmds, cmd)
	cmds = append(cmds, args...)
	rm.replicasCommands <- cmds
}

func (rm *ReplicasManager) NotifyReplicas() {
	for cmds := range rm.replicasCommands {
		func() {
			rm.replicasConnMutex.RLock()
			defer rm.replicasConnMutex.RUnlock()
			for i, conn := range rm.replicasConn {
				// TODO: non-blocking ??
				slog.Debug("notify replica", "replica_id", i)
				conn.Write([]byte(respCommand(cmds[0], cmds[1:]...)))
			}
		}()
	}
}
