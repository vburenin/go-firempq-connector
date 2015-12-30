package pqclient

import (
	"errors"
	"fmt"
	"net"

	. "github.com/vburenin/firempq_connector/api"
	. "github.com/vburenin/firempq_connector/encoders"
	. "github.com/vburenin/firempq_connector/netutils"
	. "github.com/vburenin/firempq_connector/parsers"
)

type PriorityQueueMessage struct {
	Id       string
	Payload  string
	ExpireTs int64
	UnlockTs int64
	PopCount int64
}

type PriorityQueue struct {
	conn      net.Conn
	tokReader ITokenReader
	queueName string
	asyncPop  map[string]func([]PriorityQueueMessage, error)
}

var CMD_PUSH = []byte("PUSH")
var CMD_POP = []byte("POP")
var CMD_CTX = []byte("CTX")
var CMD_SET_PARAM = []byte("SET_PARAM")
var PARAM_PAYLOAD = []byte("PL")

func NewPriorityQueue(queueName string, conn net.Conn, tokReader ITokenReader) (*PriorityQueue, error) {
	pq := &PriorityQueue{
		conn:      conn,
		tokReader: tokReader,
		queueName: queueName,
	}
	return pq.initContext(queueName)
}

func (self *PriorityQueue) GetName() string {
	return self.queueName
}

func (self *PriorityQueue) NewMessage(payload string) *PQPushMessage {
	return NewPQPushMessage(payload)
}

func (self *PriorityQueue) Push(msg *PQPushMessage) error {
	e := msg.encode()
	if err := SendCommand(self.conn, CMD_PUSH, e...); err != nil {
		return err
	}
	return self.handleOk()
}

func (self *PriorityQueue) initContext(queueName string) (*PriorityQueue, error) {
	if err := SendCommand(self.conn, CMD_CTX, EncodeString(queueName)); err != nil {
		return nil, err
	}

	if err := self.handleOk(); err != nil {
		return nil, err
	}
	return self, nil
}

func (self *PriorityQueue) handleOk() error {
	tokens, err := self.tokReader.ReadTokens()
	if err != nil {
		return err
	}
	if tokens[0] == "+OK" {
		return nil
	}
	if err := ParseError(tokens); err != nil {
		return err
	}
	return errors.New(fmt.Sprintf("Unexpected response: %s", tokens))
}
