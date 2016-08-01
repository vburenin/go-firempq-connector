package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"bufio"

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
	if c, _, _, err := fmc.makeConn(); err != nil {
		return nil, err
	} else {
		c.Close()
		return fmc, nil
	}
}

func (fmc *FireMpqClient) GetVersion() string {
	return fmc.version
}

func (fmc *FireMpqClient) makeConn() (net.Conn, *bufio.Writer, *TokenReader, error) {
	conn, err := fmc.connFactory()
	if err != nil {
		return nil, nil, nil, err
	}

	tokReader := NewTokenReader(conn)
	connHdr, err := tokReader.ReadTokens()

	if err != nil {
		return nil, nil, nil, err
	}

	if len(connHdr) == 2 && connHdr[0] == "+HELLO" {
		// TODO(vburenin): Add version check and log warning if version accidentaly changes.
		fmc.version = connHdr[1]
	} else {
		return nil, nil, nil, NewFireMpqError(-3, fmt.Sprintf("Unexpected hello string: %s", connHdr))
	}
	bufWriter := bufio.NewWriter(conn)
	return conn, bufWriter, tokReader, nil
}

func (fmc *FireMpqClient) GetPQueue(queueName string) (*PriorityQueue, error) {
	_, bufWriter, tokReader, err := fmc.makeConn()
	if err != nil {
		return nil, err
	}
	return SetPQueueContext(queueName, bufWriter, tokReader)
}

func (fmc *FireMpqClient) CreatePQueue(queueName string, opts *PqParams) (*PriorityQueue, error) {
	_, bufWriter, tokReader, err := fmc.makeConn()
	if err != nil {
		return nil, err
	}
	return CreatePQueue(queueName, bufWriter, tokReader, opts)
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

func update() {
	c, err := NewFireMpqClient("tcp", "127.0.0.1:8222")
	if err != nil {
		log.Fatal(err.Error())
	}

	pq, err := c.GetPQueue("my")
	if err != nil {
		log.Fatal(err.Error())
	}
	//v := pq.SetParams(NewPQueueOptions().SetDelay(0).SetPopLimit(10).SetMsgTtl(5000))
	//if v != nil {
	//	log.Fatal(v.Error())
	//}
	l := 0
	st := time.Now()
	for i := 0; i < 2; i++ {
		l++

		if l == 10000 {
			et := time.Now()
			d := float64(et.UnixNano()-st.UnixNano()) / 1000000000
			println(int64(10000.0 / d))
			l = 0
			st = time.Now()
		}

		err = pq.Push(pq.NewMessage("asdasdasdasdas"))
		if err != nil {
			log.Fatal(err.Error())
		}
		msg, err := pq.PopLock(nil)
		if err != nil {
			log.Fatal(err.Error())
		}
		if len(msg) == 0 {
			return
		}
		err = pq.DeleteByReceipt(msg[0].Receipt)
		if err != nil {
			log.Fatal(err.Error())
		}

	}
}

func main() {
	update()
}
