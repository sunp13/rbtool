package rbtool

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// MyConsumer ...
type MyConsumer struct {
	url       string
	queueName string

	conn  *amqp.Connection
	log   *Logger
	ready bool

	// 默认参数
	NoAck     bool // only false can use reject and ack
	Exclusive bool
	Requeue   bool
	Argument  amqp.Table
}

// NewConsumer ...
func NewConsumer(URL, qname string) *MyConsumer {
	c := &MyConsumer{
		url:       URL,
		queueName: qname,
		log:       NewDefaultLogger(),
	}
	return c
}

// Dial ...
func (c *MyConsumer) Dial() error {
	c.conn = nil
	c.log.Info("rbtool begin dial %s ...", c.url)
	// 连接3秒超时
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := make(chan *amqp.Connection, 0)
	eh := make(chan error, 0)
	go func() {
		defer func() {
			close(ch)
			close(eh)
		}()

		conn, err := amqp.Dial(c.url)
		if err != nil {
			eh <- err
			return
		}
		ch <- conn
		return
	}()

	select {
	case <-ctx.Done():
		c.log.Info("rbtool dial %s error timeout in 3sec", c.url)
		return fmt.Errorf("dial %s error timeout in 3sec", c.url)
	case err := <-eh:
		c.log.Error("rbtool dial %s error %s ", c.url, err.Error())
		return err
	case conn := <-ch:
		c.log.Info("rbtool dial %s succ", c.url)
		c.conn = conn
		c.ready = true
		return nil
	}
}

// Consume ...
func (c *MyConsumer) Consume(fn func(msg []byte) error, consumerCount int, prefetchCount int) {
	wg := &sync.WaitGroup{}
	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go c.consumerWork(wg, fn, prefetchCount, i)
	}
	wg.Wait()
}

// consumerWork 消费
func (c *MyConsumer) consumerWork(w *sync.WaitGroup, fn func(msg []byte) error, prefetchCount, i int) {
	defer w.Done()
	if !c.ready {
		c.log.Error("cosnumer id=%d, error %s", i, "conn is not ready")
		return
	}

	channel, err := c.conn.Channel()
	if err != nil {
		c.log.Error("cosnumer id=%d, error %s", i, err.Error())
		return
	}

	err = channel.Qos(prefetchCount, 0, false)
	if err != nil {
		c.log.Error("cosnumer id=%d, error %s", i, err.Error())
		return
	}
	defer channel.Close()

	hostname, _ := os.Hostname()

	deliveries, err := channel.Consume(
		c.queueName,                       // name
		fmt.Sprintf("%s_%d", hostname, i), // consumerTag,
		c.NoAck,                           // noAck   if false can use reject 只有需要ack的时候才能开启reject
		c.Exclusive,                       // exclusive
		false,                             // noLocal
		false,                             // noWait
		c.Argument,                        // arguments
	)

	if err != nil {
		c.log.Error("cosnumer id=%d, error %s", i, err.Error())
		return
	}

	for deliver := range deliveries {

		if c.log.LogSucc {
			c.log.Info("consumer id=%d rec msg %s", i, string(deliver.Body))
		}

		err := fn(deliver.Body)
		if err != nil {
			c.log.Error("consumer id=%d,rec msg %s handle error %s", i, string(deliver.Body), err.Error())
			if !c.NoAck {
				// 需要ack
				// 可以使用reject, 根据用户的requeue来决定是否requeue还是deadletter
				err = deliver.Reject(c.Requeue)
				if err != nil {
					c.log.Error("consumer id=%d,rec msg %s handle error and reject(%v) failed %s", i, string(deliver.Body), c.Requeue, err.Error())
				} else {
					c.log.Error("consumer id=%d,rec msg %s handle error and reject(%v) succ", i, string(deliver.Body), c.Requeue)
				}
			}
			continue
		}

		if !c.NoAck {
			err = deliver.Ack(false)
			if err != nil {
				c.log.Error("consumer id=%d,rec msg succ %s handle succ but Ack failed %s", i, string(deliver.Body), c.Requeue, err.Error())
			} else {
				if c.log.LogSucc {
					c.log.Info("consumer id=%d,rec msg succc %s handle succ and Ack succ", i, string(deliver.Body))
				}
			}
		}
	}
	c.log.Error("consumer id=%d, deliver process finish!")
}

// Close ...
func (c *MyConsumer) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// SetLogger ...
func (c *MyConsumer) SetLogger(logger *Logger) {
	c.log = logger
}
