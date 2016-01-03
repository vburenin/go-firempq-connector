package pqclient

import (
	"net"

	. "github.com/vburenin/firempq_connector/api"
	. "github.com/vburenin/firempq_connector/encoders"
	. "github.com/vburenin/firempq_connector/fmpq_err"
	. "github.com/vburenin/firempq_connector/netutils"
	. "github.com/vburenin/firempq_connector/parsers"
)

type PriorityQueue struct {
	conn      net.Conn
	tokReader ITokenReader
	queueName string
	asyncPop  map[string]func([]PriorityQueueMessage, error)
}

var cmdPush = []byte("PUSH")
var cmdPop = []byte("POP")
var cmdPopLock = []byte("POPLCK")
var cmdCtx = []byte("CTX")
var cmdCrt = []byte("CRT")
var cmdSetParams = []byte("SET_PARAM")

var svcPqueueType = []byte("pqueue")

func SetPQueueContext(queueName string, conn net.Conn, tokReader ITokenReader) (*PriorityQueue, error) {
	pq := &PriorityQueue{
		conn:      conn,
		tokReader: tokReader,
		queueName: queueName,
	}
	return pq.initContext(queueName)
}

func CreatePQueue(queueName string, conn net.Conn, tokReader ITokenReader,
	opts *PqOptions) (*PriorityQueue, error) {

	if err := SendIncompleteData(conn, cmdCrt, []byte(queueName), svcPqueueType); err != nil {
		return nil, err
	}

	if err := SendCompleteData(conn, opts.MakeParams()...); err != nil {
		return nil, err
	}

	if err := HandleOk(tokReader); err != nil {
		return nil, err
	}

	return SetPQueueContext(queueName, conn, tokReader)
}

func (self *PriorityQueue) GetName() string {
	return self.queueName
}

func (self *PriorityQueue) NewMessage(payload string) *PQPushMessage {
	return NewPQPushMessage(payload)
}

func (self *PriorityQueue) Push(msg *PQPushMessage) error {
	e := msg.encode()
	if err := SendCommand(self.conn, cmdPush, e...); err != nil {
		return err
	}
	return HandleOk(self.tokReader)
}

// Pop pops available from the queue completely removing them.
func (self *PriorityQueue) Pop(opts *popOptions) ([]*PriorityQueueMessage, error) {
	if err := SendCommand(self.conn, cmdPop, opts.MakeParams()...); err != nil {
		return nil, err
	}

	return self.handleMessages()
}

// PopLock pops available from the queue locking them.
func (self *PriorityQueue) PopLock(opts *popLockOptions) ([]*PriorityQueueMessage, error) {
	if err := SendCommand(self.conn, cmdPopLock, opts.MakeParams()...); err != nil {
		return nil, err
	}

	return self.handleMessages()
}

func (self *PriorityQueue) handleMessages() ([]*PriorityQueueMessage, error) {
	tokens, err := self.tokReader.ReadTokens()

	if err != nil {
		return nil, err
	}

	if tokens[0] == "+MSGS" {
		return parsePoppedMessages(tokens[1:])
	}

	if err := ParseError(tokens); err != nil {
		return nil, err
	}

	return nil, UnexpectedResponse(tokens)
}

func (self *PriorityQueue) initContext(queueName string) (*PriorityQueue, error) {
	if err := SendCommand(self.conn, cmdCtx, EncodeString(queueName)); err != nil {
		return nil, err
	}

	if err := HandleOk(self.tokReader); err != nil {
		return nil, err
	}
	return self, nil
}
