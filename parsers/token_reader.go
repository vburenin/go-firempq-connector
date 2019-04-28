package parsers

import (
	"errors"
	"io"
	"net"
	"strconv"
)

const (
	stateParseTextToken     = 1
	stateParseBinaryPayload = 2
	symbolCr                = 0x0A
)

const (
	maxRecvBufferSize  = 4096
	maxBinaryTokenLen  = 128 * 1024 * 1024
	startAsciiRange    = 0x21
	endAsciiRange      = 0x7E
	initTokenBufferLen = 48
)

var ErrTokParsingError = errors.New("Error during token parsing")

type TokenReader struct {
	buffer []byte
	bufPos int
	bufLen int
	reader io.Reader
}

func NewTokenReader(conn net.Conn) *TokenReader {
	tok := TokenReader{
		buffer: make([]byte, maxRecvBufferSize),
		bufPos: 0,
		bufLen: 0,
		reader: conn,
	}
	return &tok
}

func (tok *TokenReader) ReadTokens() ([]string, error) {
	var err error
	var token []byte = make([]byte, 0, initTokenBufferLen)
	var binTokenLen int
	var state int = stateParseTextToken

	result := make([]string, 0, 4)

	for {
		if tok.bufPos >= tok.bufLen {
			// Read more data from the network reader
			tok.bufPos = 0
			tok.bufLen, err = tok.reader.Read(tok.buffer)
			if nil != err {
				return nil, err
			}
		}

		// Tokenize content of the buffer
		for tok.bufPos < tok.bufLen {
			if state == stateParseBinaryPayload {
				availableBytes := tok.bufLen - tok.bufPos
				if availableBytes > binTokenLen {
					availableBytes = binTokenLen
				}
				token = append(token, tok.buffer[tok.bufPos:tok.bufPos+availableBytes]...)
				binTokenLen -= availableBytes

				tok.bufPos += availableBytes

				if binTokenLen <= 0 {
					// Binary token complete
					state = stateParseTextToken
					result = append(result, string(token))
					token = make([]byte, 0, initTokenBufferLen)
				}
				continue
			}

			val := tok.buffer[tok.bufPos]
			tok.bufPos += 1

			if val >= startAsciiRange && val <= endAsciiRange {
				token = append(token, val)
			} else if len(token) > 0 {
				if token[0] == '$' {
					binTokenLen, err = strconv.Atoi(string(token[1:]))
					if err == nil && (binTokenLen < 1 || binTokenLen > maxBinaryTokenLen) {
						return nil, ErrTokParsingError
					}
					state = stateParseBinaryPayload
					token = make([]byte, 0, binTokenLen)
				} else {
					result = append(result, string(token))
					if val == symbolCr {
						return result, nil
					}
					token = make([]byte, 0, initTokenBufferLen)
				}
			} else {
				if val == symbolCr {
					return result, nil
				}
			}
		}
	}
}
