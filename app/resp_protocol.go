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

func respInt(n int) string {
	return fmt.Sprintf(":%d", n)
}

func respCommand(cmd string, args ...string) string {
	res := strings.Builder{}
	res.WriteString(fmt.Sprintf("*%d\r\n$%d\r\n%s", len(args)+1, len(cmd), cmd))
	for _, arg := range args {
		res.WriteString(fmt.Sprintf("\r\n$%d\r\n%s", len(arg), arg))
	}
	res.WriteString("\r\n")
	return res.String()
}
