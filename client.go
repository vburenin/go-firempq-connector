package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	. "github.com/vburenin/firempq_connector/fmpq_err"
	. "github.com/vburenin/firempq_connector/parsers"
	. "github.com/vburenin/firempq_connector/pqclient"
)

type FireMpqClient struct {
	connFactory func() (net.Conn, error)
	version     string
}

// NewFireMpqClient makes a first connection to the service to ensure service availability
// and returns a client instance.
func NewFireMpqClient(network, address string) (*FireMpqClient, error) {
	factory := func() (net.Conn, error) {
		return net.Dial(network, address)
	}

	fmc := &FireMpqClient{connFactory: factory}
	if c, _, err := fmc.makeConn(); err != nil {
		return nil, err
	} else {
		c.Close()
		return fmc, nil
	}
}

func (self *FireMpqClient) GetVersion() string {
	return self.version
}

func (self *FireMpqClient) makeConn() (net.Conn, *TokenReader, error) {
	conn, err := self.connFactory()
	if err != nil {
		return nil, nil, err
	}

	tokReader := NewTokenReader(conn)
	connHdr, err := tokReader.ReadTokens()

	if err != nil {
		return nil, nil, err
	}

	if len(connHdr) == 2 && connHdr[0] == "+HELLO" {
		// TODO(vburenin): Add version check and log warning if version accidentaly changes.
		self.version = connHdr[1]
	} else {
		return nil, nil, NewFireMpqError(-3, fmt.Sprintf("Unexpected hello string: %s", connHdr))
	}
	return conn, tokReader, nil
}

func (self *FireMpqClient) GetPQueue(queueName string) (*PriorityQueue, error) {
	conn, tokReader, err := self.makeConn()
	if err != nil {
		return nil, err
	}
	return NewPriorityQueue(queueName, conn, tokReader)
}

var last_ts int64
var lck sync.Mutex
var counter int64

const MSG_T = 20000

func inc() {
	lck.Lock()
	counter++
	if counter >= MSG_T {
		counter = 0
		prev_ts := last_ts
		last_ts = time.Now().UnixNano()
		lck.Unlock()

		tdelta := float64(last_ts-prev_ts) / 1000000000.0

		fmt.Println(MSG_T / tdelta)

	} else {
		lck.Unlock()

	}

}

func getCtx() *PriorityQueue {
	c, err := NewFireMpqClient("tcp", "127.0.0.1:9033")
	if err != nil {
		log.Fatal(err.Error())
	}

	pq, err := c.GetPQueue("c")
	if err != nil {
		log.Fatal(err.Error())
	}
	return pq
}

func pusher() {
	pq := getCtx()
	msg := pq.NewMessage("some data")
	for {
		if err := pq.Push(msg); err != nil {
			log.Fatal(err.Error())
		}
		inc()
	}

}

func pushAndPop() {
	pq := getCtx()
	msg := pq.NewMessage("asdasdasd")
	if err := pq.Push(msg); err != nil {
		log.Fatal(err.Error())
	}
	msg = pq.NewMessage("asdasdasd")
	if err := pq.Push(msg); err != nil {
		log.Fatal(err.Error())
	}
	m, err := pq.Pop(NewPopOptions().SetLimit(2))
	fmt.Printf("%s", m)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func main() {
	last_ts = time.Now().UnixNano()
	//for i := 0; i < 500; i++ {
	//	go pusher()
	//}
	//time.Sleep(time.Second * 1000)
	pushAndPop()
}
