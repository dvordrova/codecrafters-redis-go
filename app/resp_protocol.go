package main

import (
	"fmt"
	"strings"
)

func respError(msg string) string {
	return fmt.Sprintf("-%s\r\n", msg)
}

func respString(msg string) string {
	return fmt.Sprintf("+%s\r\n", msg)
}

func respBulkString(msg ...string) string {
	if len(msg) > 0 {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(msg[0]), msg[0])
	} else {
		return "$-1\r\n"
	}
}

func respCommand(msgs ...string) string {
	res := strings.Builder{}
	res.WriteString(fmt.Sprintf("*%d", len(msgs)))
	for _, msg := range msgs {
		res.WriteString(fmt.Sprintf("\r\n$%d\r\n%s", len(msg), msg))
	}
	res.WriteString("\r\n")
	return res.String()
}
