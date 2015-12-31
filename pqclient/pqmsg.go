package pqclient

import (
	. "github.com/vburenin/firempq_connector/encoders"
	. "github.com/vburenin/firempq_connector/fmpq_err"
	. "github.com/vburenin/firempq_connector/parsers"
)

var PRM_ID = []byte("ID")
var PRM_POP_WAIT = []byte("WAIT")
var PRM_LOCK_TIMEOUT = []byte("TIMEOUT")
var PRM_PRIORITY = []byte("PRIORITY")
var PRM_LIMIT = []byte("LIMIT")
var PRM_PAYLOAD = []byte("PL")
var PRM_DELAY = []byte("DELAY")
var PRM_TIMESTAMP = []byte("TS")
var PRM_ASYNC = []byte("ASYNC")
var PRM_SYNC_WAIT = []byte("SYNCWAIT")
var PRM_MSG_TTL = []byte("TTL")


type PQPushMessage struct {
	id       string
	priority int64
	payload  string
	delay    int64
	ttl      int64
	syncWait bool
	async    bool
}

func NewPQPushMessage(payload string) *PQPushMessage {
	return &PQPushMessage{
		payload:  payload,
		id:       "",
		priority: 0,
		delay:    -1,
		ttl:      -1,
		syncWait: false,
		async:    false,
	}
}

func (self *PQPushMessage) SetId(id string) *PQPushMessage {
	self.id = id
	return self
}

func (self *PQPushMessage) SetPriority(priority int64) *PQPushMessage {
	self.priority = priority
	return self
}

func (self *PQPushMessage) SetDelay(delay uint64) *PQPushMessage {
	self.delay = int64(delay)
	return self
}

func (self *PQPushMessage) SetTtl(ttl uint64) *PQPushMessage {
	self.ttl = int64(ttl)
	return self
}

func (self *PQPushMessage) SetSyncWait(b bool) *PQPushMessage {
	self.syncWait = b
	return self
}

func (self *PQPushMessage) SetAsync(b bool) *PQPushMessage {
	self.async = b
	return self
}

func (self *PQPushMessage) encode() [][]byte {
	data := make([][]byte, 0, 2)
	if self.id != "" {
		data = append(data, PRM_ID)
		data = append(data, EncodeString(self.id))
	}
	if self.priority != 0 {
		data = append(data, PRM_PRIORITY)
		data = append(data, EncodeInt64(self.priority))
	}
	if self.delay >= 0 {
		data = append(data, PRM_DELAY)
		data = append(data, EncodeInt64(self.delay))
	}
	if self.ttl >= 0 {
		data = append(data, PRM_MSG_TTL)
		data = append(data, EncodeInt64(self.ttl))
	}
	if self.syncWait {
		data = append(data, PRM_SYNC_WAIT)
	}

	data = append(data, PRM_PAYLOAD)
	data = append(data, EncodeString(self.payload))
	return data
}

type PriorityQueueMessage struct {
	Id       string
	Payload  string
	ExpireTs int64
	UnlockTs int64
	PopCount int64
}

func parsePoppedMessages(tokens []string) ([]*PriorityQueueMessage, error) {
	if len(tokens) == 0 {
		WrongMessageFormatError("No array header")
	}
	arraySize, err := ParseArraySize(tokens[0])
	msgs := make([]*PriorityQueueMessage, 0, arraySize)
	if err != nil {
		return nil, err
	}
	tokens = tokens[1:]
	for i := arraySize; i > 0; i-- {
		if len(tokens) == 0 {
			WrongMessageFormatError("Array with messages ends unexpectedly")
		}
		keysCount, err := ParseMapSize(tokens[0])
		if err != nil {
			return nil, err
		}
		tokens = tokens[1:]
		tokensNeeded := keysCount << 1
		if int64(len(tokens)) < tokensNeeded {
			return nil, WrongMessageFormatError("Message data ends unexpectedly")
		}
		msg, err := parseMessage(tokens[:tokensNeeded])
		if err != nil {
			return nil, err
		}
		tokens = tokens[tokensNeeded:]
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func parseMessage(tokens []string) (*PriorityQueueMessage, error) {
	msg := PriorityQueueMessage{}
	var err error
	idx := len(tokens) - 2
	for idx >= 0 {
		switch tokens[idx] {
		case "ID":
			msg.Id = tokens[idx+1]
		case "PL":
			msg.Payload = tokens[idx+1]
		case "UTS":
			msg.UnlockTs, err = ParseInt(tokens[idx+1])
		case "ETS":
			msg.ExpireTs, err = ParseInt(tokens[idx+1])
		case "POPCNT":
			msg.PopCount, err = ParseInt(tokens[idx+1])
		default:
		}
		if err != nil {
			return nil, err
		}
		idx -= 2
	}
	return &msg, nil
}
