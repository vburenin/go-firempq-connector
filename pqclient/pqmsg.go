package pqclient

import "github.com/vburenin/firempq_connector/encoders"

type PQPushMessage struct {
	id       string
	payload  string
	delay    int64
	ttl      int64
	syncWait bool
	async    bool
}

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

func NewPQPushMessage(payload string) *PQPushMessage {
	return &PQPushMessage{
		payload:  payload,
		id:       "",
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
		data = append(data, encoders.EncodeString(self.id))
	}
	if self.delay >= 0 {
		data = append(data, PRM_DELAY)
		data = append(data, encoders.EncodeInt64(self.delay))
	}
	if self.ttl >= 0 {
		data = append(data, PRM_MSG_TTL)
		data = append(data, encoders.EncodeInt64(self.ttl))
	}
	if self.syncWait {
		data = append(data, PRM_SYNC_WAIT)
	}

	data = append(data, PRM_PAYLOAD)
	data = append(data, encoders.EncodeString(self.payload))
	return data
}
