package netutils

import "bufio"

func CompleteWrite(writer *bufio.Writer) error {
	writer.WriteByte('\n')
	return writer.Flush()
}

func SendData(writer *bufio.Writer, data ...[]byte) error {
	var err error
	for _, d := range data {
		err = writer.WriteByte(' ')
		_, err = writer.Write(d)
	}
	return err
}

func SendCommand(writer *bufio.Writer, cmd string, data ...[]byte) error {
	writer.WriteString(cmd)
	SendData(writer, data...)
	return CompleteWrite(writer)
}

func WriteData(writer *bufio.Writer, cmd string, data [][]byte) (err error) {
	writer.WriteString(cmd)
	for _, d := range data {
		writer.WriteByte(' ')
		_, err = writer.Write(d)
	}
	return err
}
