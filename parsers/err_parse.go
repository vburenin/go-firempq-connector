package parsers

import (
	"fmt"

	. "github.com/vburenin/firempq_connector/fmpq_err"
)

func UnexpectedErrorFormat(tokens []string) *FireMpqError {
	return NewFireMpqError(-1, fmt.Sprintf("Unexpected error format: %s", tokens))
}

func ParseError(tokens []string) *FireMpqError {
	if tokens[0] == "-ERR" {
		if len(tokens) != 3 {
			return UnexpectedErrorFormat(tokens)
		}
		if errcode, err := ParseInt(tokens[1]); err != nil {
			return UnexpectedErrorFormat(tokens)
		} else {
			return NewFireMpqError(errcode, tokens[2])
		}
	}
	return nil
}