package parsers

import (
	. "github.com/vburenin/firempq_connector/api"
	. "github.com/vburenin/firempq_connector/fmpq_err"
)

func HandleOk(tokReader ITokenReader) error {
	tokens, err := tokReader.ReadTokens()
	if err != nil {
		return err
	}
	if tokens[0] == "+OK" {
		return nil
	}
	if err := ParseError(tokens); err != nil {
		return err
	}
	return UnexpectedResponse(tokens)
}
