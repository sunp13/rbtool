package rbtool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Producer 生产者
type Producer struct {
	URL string

	conn             *amqp.Connection
	channelConfirmed *amqp.Channel
	channel          *amqp.Channel
	log              *Logger
	lock             sync.RWMutex
}

// NewProducer ...
func NewProducer(url string) *Producer {
	return &Producer{
		URL: url,
		log: NewDefaultLogger(),
	}
}

// Dial ... 连接
func (p *Producer) Dial() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.conn = nil
	p.channelConfirmed = nil
	p.channel = nil

	p.log.Info("rbtool begin dial %s ...", p.URL)
	// 连接3秒超时
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c := make(chan *amqp.Connection, 0)
	e := make(chan error, 0)
	go func() {
		defer func() {
			close(c)
			close(e)
		}()

		conn, err := amqp.Dial(p.URL)
		if err != nil {
			e <- err
			return
		}
		c <- conn
		return
	}()

	select {
	case <-ctx.Done():
		p.log.Info("rbtool dial %s error timeout in 3sec", p.URL)
		return fmt.Errorf("dial %s error timeout in 3sec", p.URL)
	case err := <-e:
		p.log.Error("rbtool dial %s error %s ", p.URL, err.Error())
		return err
	case conn := <-c:
		p.log.Info("rbtool dial %s succ", p.URL)
		p.conn = conn
		var err error
		// 开启一个不需要confirmed 的 channel
		p.channel, err = conn.Channel()
		if err != nil {
			return err
		}
		// 开启需要confirmed 的 channel
		p.channelConfirmed, err = conn.Channel()
		if err != nil {
			return err
		}
		if err = p.channelConfirmed.Confirm(false); err != nil {
			return err
		}
		return nil
	}
}

// PublishConfirmed ... 发布同步消息-> 等待confirm ack
func (p *Producer) PublishConfirmed(exchange, key string, msg amqp.Publishing) error {
	// 如果channel 不存在, 尝试重连
	// if p.syncChannel == nil {
	// 	for {
	// 		if err := p.Dial(); err == nil {
	// 			break
	// 		}
	// 	}
	// }
	//

	err := p.channelConfirmed.Publish(
		exchange,
		key,
		false,
		false,
		msg,
	)
	if err != nil {
		// 判断是否是网络问题要重新连接
		return err
	}
	confirm := p.channelConfirmed.NotifyPublish(make(chan amqp.Confirmation, 0))
	if confirmed := <-confirm; confirmed.Ack {
		if p.log.LogSucc {
			p.log.Info("Publish confirm msg succ! ex=%s,key=%s,msg=%s", exchange, key, string(msg.Body))
		}
		return nil
	}
	p.log.Error("Publish confirm msg failed no confirmed ex=%s,key=%s,msg=%s", exchange, key, string(msg.Body))
	return fmt.Errorf("Publish confirm msg failed no confirmed ex=%s,key=%s,msg=%s", exchange, key, string(msg.Body))
}

// Publish 不需要confirm的publish
func (p *Producer) Publish(exchange, key string, msg amqp.Publishing) error {
	err := p.channel.Publish(
		exchange,
		key,
		false,
		false,
		msg,
	)
	if err != nil {
		// 判断是否要重新连接
		p.log.Error("Publish noconfirm msg failed ex=%s,key=%s,msg=%s,err=%s", exchange, key, string(msg.Body), err.Error())
		return err
	}
	if p.log.LogSucc {
		p.log.Info("Publish noconfirm msg succ! ex=%s,key=%s,msg=%s", exchange, key, string(msg.Body))
	}
	return nil
}

// SetLogger 设置logger
func (p *Producer) SetLogger(logger *Logger) {
	p.log = logger
}
