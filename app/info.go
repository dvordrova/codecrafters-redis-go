package main

import (
	"fmt"
)

type RedisInfo struct {
	replication replicationInfo
}

func genMasterReplId() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

func NewRedisInfo(role string) RedisInfo {
	return RedisInfo{
		replicationInfo{
			role:             role,
			masterReplId:     genMasterReplId(),
			masterReplOffset: 0,
		},
	}
}

func (info *RedisInfo) String() string {
	return fmt.Sprintf("%s", &info.replication)
}

func (info *RedisInfo) GetMasterReplId() string {
	return info.replication.masterReplId
}

type replicationInfo struct {
	role             string
	masterReplId     string
	masterReplOffset int
}

func (replication *replicationInfo) String() string {
	return fmt.Sprintf(
		`# Replication
role:%s
master_replid:%s
master_repl_offset:%d
`, replication.role,
		replication.masterReplId,
		replication.masterReplOffset,
	)
}
