package parsers

import . "github.com/vburenin/firempq_connector/fmpq_err"

func ParseError(tokens []string) error {
	if tokens[0] == "-ERR" {
		if len(tokens) < 3 {
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
