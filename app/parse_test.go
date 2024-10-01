package main

import (
	"slices"
	"testing"
)

func TestParseCommand(t *testing.T) {
	for _, test := range []struct {
		request        string
		expectedResult []string
		expectedError  error
	}{
		{
			request:        "*1\r\n$4\r\nPING\r\n",
			expectedResult: []string{"ping"},
		},
		{
			request:        "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
			expectedResult: []string{"echo", "hey"},
		},
	} {
		result, err := parseCommand([]byte(test.request))
		if !slices.Equal(result, test.expectedResult) || err != test.expectedError {
			t.Logf(
				"for %v expected %v, %v, but got %v, %v",
				test.request,
				test.expectedResult,
				test.expectedError,
				result,
				err,
			)
			t.Fail()
		}
	}
}
