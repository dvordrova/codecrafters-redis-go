package main

import "fmt"

func respError(msg string) string {
	return fmt.Sprintf("-%s\r\n")
}

func respString(msg string) string {
	return fmt.Sprintf("+%s\r\n")
}

func respBulkString(msg ...string) string {
	if len(msg) > 0 {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(msg[0]), msg[0])
	} else {
		return "$-1\r\n"
	}
}
