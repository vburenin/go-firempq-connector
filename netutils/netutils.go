package netutils

import (
	"io"
	"net"
)

var ENDL = []byte("\n")
var SPACE = []byte(" ")

func SendCompleteData(writer io.Writer, data ...[]byte) error {
	l := len(data) - 1
	for idx, d := range data {
		if _, err := writer.Write(d); err != nil {
			return err
		}
		if idx < l {
			if _, err := writer.Write(SPACE); err != nil {
				return err
			}
		}
	}
	if _, err := writer.Write(ENDL); err != nil {
		return err
	}
	return nil
}

func SendIncompleteData(writer net.Conn, data ...[]byte) error {
	for _, d := range data {
		if _, err := writer.Write(d); err != nil {
			return err
		}
		if _, err := writer.Write(SPACE); err != nil {
			return err
		}
	}
	return nil
}

func SendCommand(writer net.Conn, cmd []byte, data ...[]byte) error {
	err := SendIncompleteData(writer, cmd)
	if err == nil {
		err = SendCompleteData(writer, data...)
	}
	return err
}
