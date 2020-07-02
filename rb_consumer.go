package rbtool

import (
	"context"
	"fmt"
	"regexp"
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
	reconnect chan struct{}
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
	c.ready = false
	c.conn = nil
	c.log.Info("rbtool begin dial ...")
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
		c.log.Info("rbtool dial error connected party did not properly respond after a period of time in 3sec")
		return fmt.Errorf("dial error connected party did not properly respond after a period of time in 3sec")
	case err := <-eh:
		c.log.Error("rbtool dial error %s ", err.Error())
		return err
	case conn := <-ch:
		c.log.Info("rbtool dial succ")
		c.conn = conn
		c.ready = true
		c.reconnect = make(chan struct{})

		go func() {
			<-c.reconnect
			c.log.Error("rbtool begin reconnect...")
			for {
				err := c.Dial()
				if err == nil {
					break
				}
				time.Sleep(1 * time.Second)
			}
		}()

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
	defer func() {
		if err := recover(); err != nil {
			// fmt.Printf("%s\n", err)
		}
	}()

	defer w.Done()
	if !c.ready {
		c.log.Error("cosnumer id=%d, error %s", i, "conn is not ready")
		return
	}

	channel, err := c.conn.Channel()
	if err != nil {
		if matched, _ := regexp.MatchString(`connected party did not properly respond after a period of time`, err.Error()); matched {
			close(c.reconnect)
		}
		if matched, _ := regexp.MatchString(`channel/connection is not open`, err.Error()); matched {
			close(c.reconnect)
		}
		c.log.Error("cosnumer id=%d, error %s", i, err.Error())
		return
	}

	err = channel.Qos(prefetchCount, 0, false)

	if err != nil {
		c.log.Error("cosnumer id=%d, error %s", i, err.Error())
		return
	}
	defer channel.Close()

	deliveries, err := channel.Consume(
		c.queueName, // name
		"",          // consumerTag,
		c.NoAck,     // noAck   if false can use reject 只有需要ack的时候才能开启reject
		c.Exclusive, // exclusive
		false,       // noLocal
		false,       // noWait
		c.Argument,  // arguments
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
	c.log.Error("consumer id=%d, deliver process finish!", i)
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
