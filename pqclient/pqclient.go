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

var (
	cmdPush             = []byte("PUSH")
	cmdPop              = []byte("POP")
	cmdPopLock          = []byte("POPLCK")
	cmdCtx              = []byte("CTX")
	cmdCrt              = []byte("CRT")
	cmdSetCfg           = []byte("SETCFG")
	cmdDeleteById       = []byte("DEL")
	cmdDeleteByReceipt  = []byte("RDEL")
	cmdDeleteLockedById = []byte("DELLCK")
	cmdUnlockById       = []byte("UNLCK")
	cmdUnlockByReceipt  = []byte("RUNLCK")

	svcPqueueType = []byte("pqueue")
)

func SetPQueueContext(queueName string, conn net.Conn, tokReader ITokenReader) (*PriorityQueue, error) {
	if err := SendCommand(conn, cmdCtx, EncodeString(queueName)); err != nil {
		return nil, err
	}
	if err := HandleOk(tokReader); err != nil {
		return nil, err
	}
	pq := &PriorityQueue{
		conn:      conn,
		tokReader: tokReader,
		queueName: queueName,
	}
	return pq, nil
}

func CreatePQueue(queueName string, conn net.Conn, tokReader ITokenReader,
	opts *PqParams) (*PriorityQueue, error) {

	if err := SendIncompleteData(conn, cmdCrt, []byte(queueName), svcPqueueType); err != nil {
		return nil, err
	}

	if err := SendCompleteData(conn, opts.makeRequest()...); err != nil {
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
	if err := SendCommand(self.conn, cmdPop, opts.makeRequest()...); err != nil {
		return nil, err
	}

	return self.handleMessages()
}

// PopLock pops available from the queue locking them.
func (self *PriorityQueue) PopLock(opts *popLockOptions) ([]*PriorityQueueMessage, error) {
	if err := SendCommand(self.conn, cmdPopLock, opts.makeRequest()...); err != nil {
		return nil, err
	}

	return self.handleMessages()
}

func (self *PriorityQueue) DeleteById(id string) error {
	if err := SendCommand(self.conn, cmdDeleteById, EncodeString(id)); err != nil {
		return err
	}
	return HandleOk(self.tokReader)
}

func (self *PriorityQueue) DeleteLockedById(id string) error {
	if err := SendCommand(self.conn, cmdDeleteLockedById, EncodeString(id)); err != nil {
		return err
	}
	return HandleOk(self.tokReader)
}

func (self *PriorityQueue) DeleteByReceipt(rcpt string) error {
	if err := SendCommand(self.conn, cmdDeleteByReceipt, EncodeString(rcpt)); err != nil {
		return err
	}
	return HandleOk(self.tokReader)
}

func (self *PriorityQueue) UnlockById(id string) error {
	if err := SendCommand(self.conn, cmdUnlockById, EncodeString(id)); err != nil {
		return err
	}
	return HandleOk(self.tokReader)
}

func (self *PriorityQueue) UnlockByReceipt(rcpt string) error {
	if err := SendCommand(self.conn, cmdUnlockByReceipt, EncodeString(rcpt)); err != nil {
		return err
	}
	return HandleOk(self.tokReader)
}

func (self *PriorityQueue) SetParams(params *PqParams) error {
	if err := SendCommand(self.conn, cmdSetCfg, params.makeRequest()...); err != nil {
		return err
	}
	return HandleOk(self.tokReader)
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
