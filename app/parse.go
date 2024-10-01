package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

func parseCommand(request []byte) (result []string, err error) {
	var tokenLength int
	slog.Debug("start parsing", "request", request)
	if len(request) == 0 {
		err = fmt.Errorf("expecting array but got nothing")
		return
	}
	if request[0] != '*' {
		err = fmt.Errorf("expecting array prefix '*' but got %c", request[0])
		return
	}
	sep := []byte("\r\n")
	arrayInfoEnd := bytes.Index(request, sep)
	if arrayInfoEnd == -1 {
		err = fmt.Errorf("not found '\r\n'")
		return
	}
	length, err := strconv.Atoi(string(request[1:arrayInfoEnd]))
	if err != nil {
		err = fmt.Errorf("parsing length of array request: %w", err)
		return
	}
	pos := arrayInfoEnd + 2
	for i := 0; i < length; i++ {
		if pos >= len(request) {
			err = fmt.Errorf("#%d element is not presented, but expected", i)
			return
		}
		if request[pos] != '$' {
			err = fmt.Errorf("expecting bulk string prefix '$' but got %c", request[pos])
			return
		}
		tokenInfoEnd := bytes.Index(request[pos:], sep)
		if tokenInfoEnd == -1 {
			err = fmt.Errorf("not found '\r\n' after %d token", i)
			return
		}
		tokenLength, err = strconv.Atoi(string(request[pos+1 : pos+tokenInfoEnd]))
		if err != nil {
			err = fmt.Errorf("parsing length of %d token: %w", i, err)
			return
		}
		tokenStartPos := pos + tokenInfoEnd + 2
		tokenEndPos := tokenStartPos + tokenLength
		if tokenEndPos+2 > len(request) {
			err = fmt.Errorf("size of %d token is %d, but request ends", i, tokenLength)
			return
		}
		if request[tokenEndPos] != '\r' || request[tokenEndPos+1] != '\n' {
			err = fmt.Errorf("not found '\r\n' after %d token", i)
			return
		}
		token := string(request[tokenStartPos:tokenEndPos])
		if i == 0 {
			token = strings.ToLower(token)
		}
		result = append(result, token)
		pos = tokenEndPos + 2
	}

	return
}
