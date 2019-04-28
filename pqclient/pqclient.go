package pqclient

import (
	"bufio"

	. "github.com/vburenin/firempq_connector/api"
	. "github.com/vburenin/firempq_connector/encoders"
	. "github.com/vburenin/firempq_connector/fmpq_err"
	. "github.com/vburenin/firempq_connector/netutils"
	. "github.com/vburenin/firempq_connector/parsers"
)

type PriorityQueue struct {
	bufWriter *bufio.Writer
	tokReader ITokenReader
	queueName string
	asyncPop  map[string]func([]QueueMessage, error)
}

var (
	cmdPush             = "PUSH"
	cmdPushBatch        = "PUSHB"
	cmdBatchNext        = "NXT"
	cmdPop              = "POP"
	cmdPopLock          = "POPLCK"
	cmdCtx              = "CTX"
	cmdCrt              = "CRT"
	cmdSetCfg           = "SETCFG"
	cmdDeleteById       = "DEL"
	cmdDeleteByReceipt  = "RDEL"
	cmdDeleteLockedById = "DELLCK"
	cmdUnlockById       = "UNLCK"
	cmdUnlockByReceipt  = "RUNLCK"
)

type PushBatchItem struct {
	Error error
	MsgID string
}

func SetPQueueContext(queueName string, bufWriter *bufio.Writer, tokReader ITokenReader) (*PriorityQueue, error) {
	if err := SendCommand(bufWriter, cmdCtx, EncodeString(queueName)); err != nil {
		return nil, err
	}

	if err := HandleOk(tokReader); err != nil {
		return nil, err
	}

	pq := &PriorityQueue{
		bufWriter: bufWriter,
		tokReader: tokReader,
		queueName: queueName,
	}
	return pq, nil
}

func CreatePQueue(queueName string, bufWriter *bufio.Writer, tokReader ITokenReader, opts *PqParams) (*PriorityQueue, error) {

	SendCommand(bufWriter, cmdCrt, []byte(queueName))
	SendData(bufWriter, opts.makeRequest()...)
	CompleteWrite(bufWriter)

	if err := HandleOk(tokReader); err != nil {
		return nil, err
	}

	return SetPQueueContext(queueName, bufWriter, tokReader)
}

func (pq *PriorityQueue) GetName() string {
	return pq.queueName
}

func (pq *PriorityQueue) NewMessage(payload string) *Message {
	return NewMessage(payload)
}

func (pq *PriorityQueue) PushBatch(msgs ...*Message) ([]PushBatchItem, error) {
	last := len(msgs) - 1
	if last == -1 {
		return nil, nil
	}
	pushCmd := cmdPushBatch
	for i, msg := range msgs {
		WriteData(pq.bufWriter, pushCmd, msg.encode())
		if i != last {
			pq.bufWriter.WriteByte(' ')
		}
		pushCmd = cmdBatchNext
	}
	err := CompleteWrite(pq.bufWriter)
	if err != nil {
		return nil, err
	}
	return pq.handleBatchResponse()
}

func (pq *PriorityQueue) Push(msg *Message) error {
	if err := SendCommand(pq.bufWriter, cmdPush, msg.encode()...); err != nil {
		return err
	}
	return HandleOk(pq.tokReader)
}

// Pop pops available from the queue completely removing them.
func (pq *PriorityQueue) Pop(opts *popOptions) ([]*QueueMessage, error) {
	if err := SendCommand(pq.bufWriter, cmdPop, opts.makeRequest()...); err != nil {
		return nil, err
	}

	return pq.handleMessages()
}

// PopLock pops available from the queue locking them.
func (pq *PriorityQueue) PopLock(opts *popLockOptions) ([]*QueueMessage, error) {
	if err := SendCommand(pq.bufWriter, cmdPopLock, opts.makeRequest()...); err != nil {
		return nil, err
	}

	return pq.handleMessages()
}

func (pq *PriorityQueue) DeleteById(id string) error {
	if err := SendCommand(pq.bufWriter, cmdDeleteById, EncodeString(id)); err != nil {
		return err
	}
	return HandleOk(pq.tokReader)
}

func (pq *PriorityQueue) DeleteLockedById(id string) error {
	if err := SendCommand(pq.bufWriter, cmdDeleteLockedById, EncodeString(id)); err != nil {
		return err
	}
	return HandleOk(pq.tokReader)
}

func (pq *PriorityQueue) DeleteByReceipt(rcpt string) error {
	if err := SendCommand(pq.bufWriter, cmdDeleteByReceipt, EncodeString(rcpt)); err != nil {
		return err
	}
	return HandleOk(pq.tokReader)
}

func (pq *PriorityQueue) UnlockById(id string) error {
	if err := SendCommand(pq.bufWriter, cmdUnlockById, EncodeString(id)); err != nil {
		return err
	}
	return HandleOk(pq.tokReader)
}

func (pq *PriorityQueue) UnlockByReceipt(rcpt string) error {
	if err := SendCommand(pq.bufWriter, cmdUnlockByReceipt, EncodeString(rcpt)); err != nil {
		return err
	}
	return HandleOk(pq.tokReader)
}

func (pq *PriorityQueue) SetParams(params *PqParams) error {
	if err := SendCommand(pq.bufWriter, cmdSetCfg, params.makeRequest()...); err != nil {
		return err
	}
	return HandleOk(pq.tokReader)
}

func (pq *PriorityQueue) handleMessages() ([]*QueueMessage, error) {
	tokens, err := pq.tokReader.ReadTokens()

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

func (pq *PriorityQueue) handleBatchResponse() ([]PushBatchItem, error) {
	tokens, err := pq.tokReader.ReadTokens()
	if err != nil {
		return nil, err
	}
	if len(tokens) < 2 {
		return nil, UnexpectedResponse(tokens)
	}
	if tokens[0] == "+BATCH" {
		size, err := ParseArraySize(tokens[1])
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, UnexpectedResponse(tokens)
		}
		return pq.parseBatchResponse(int(size))

	}
	if err := ParseError(tokens); err != nil {
		return nil, err
	}

	return nil, UnexpectedResponse(tokens)
}

func (pq *PriorityQueue) parseBatchResponse(size int) ([]PushBatchItem, error) {
	respItems := make([]PushBatchItem, 0, size)
	var id string
	for len(respItems) < size {
		tokens, err := pq.tokReader.ReadTokens()
		if err != nil {
			return nil, err
		}

		err = ParseError(tokens)
		if err != nil {
			respItems = append(respItems, PushBatchItem{Error: err})
			continue
		}

		id, err = ParseMessageId(tokens)
		if id != "" {
			respItems = append(respItems, PushBatchItem{MsgID: id})
			tokens = tokens[2:]
			continue
		}
		return nil, UnexpectedResponse(tokens)
	}
	return respItems, nil
}
