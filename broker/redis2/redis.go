// Package redis provides a Redis broker
package redis2

import "C"
import (
	"context"
	"errors"
	"fmt"
	"log"

	//old "github.com/gomodule/redigo/redis"
	redis "github.com/go-redis/redis/v7"
	"github.com/micro/go-micro/v2/broker"
	"github.com/micro/go-micro/v2/codec"
	"github.com/micro/go-micro/v2/codec/json"
	"github.com/micro/go-micro/v2/config/cmd"
)

//var ErrNil = errors.New("go-redis: nil returned")
//type Error string

func init() {
	cmd.DefaultBrokers["redis"] = NewBroker
}

// publication is an internal publication for the Redis broker.
type publication struct {
	topic   string
	message *broker.Message
	err     error
}

// Topic returns the topic this publication applies to.
func (p *publication) Topic() string {
	return p.topic
}

// Message returns the broker message of the publication.
func (p *publication) Message() *broker.Message {
	return p.message
}

// Ack sends an acknowledgement to the broker. However this is not supported
// is Redis and therefore this is a no-op.
func (p *publication) Ack() error {
	return nil
}

func (p *publication) Error() error {
	return p.err
}

// subscriber proxies and handles Redis messages as broker publications.
type subscriber struct {
	codec  codec.Marshaler
	client   *redis.Client
	topic  string
	handle broker.Handler
	opts   broker.SubscribeOptions
}

// recv loops to receive new messages from Redis and handle them
// as publications.
func (s *subscriber) recv(channel string) {

	// Close the connection once the subscriber stops receiving.
	defer s.client.Conn().Close()

	_, err := s.client.Subscribe(channel).Receive()
	if err != nil {
		return
	}

	ch := s.client.Subscribe(channel).Channel()


	// Handle error? Only a log would be necessary since this type
	// of issue cannot be fixed.

	for {
		var m broker.Message

		msg, _ := <-ch
		fmt.Println("received msg from channel")
		if err := s.codec.Unmarshal([]byte(msg.Payload), &m); err != nil {
			log.Fatal("received message error")
		}

		p := publication{
			topic:   msg.Channel,
			message: &m,
		}
		fmt.Println(msg.Channel)
		fmt.Println(m)

		// Handle error? Retry?
		if p.err = s.handle(&p); p.err != nil {
			log.Fatal("handle message error")
		}

		// Added for posterity, however Ack is a no-op.
		if s.opts.AutoAck {
			if err := p.Ack(); err != nil {
				log.Fatal("ack message error")
			}
		}

	}


		//switch x := rcv.(type) {
		//case redis.Message:
		//	fmt.Println("message type")
		//	var m broker.Message
		//
		//	// Handle error? Only a log would be necessary since this type
		//	// of issue cannot be fixed.
		//	if err := s.codec.Unmarshal([]byte(x.String()), &m); err != nil {
		//		break
		//	}
		//
		//	p := publication{
		//		topic:   x.Channel,
		//		message: &m,
		//	}
		//	fmt.Println(x.Channel)
		//	fmt.Println(m)
		//
		//	// Handle error? Retry?
		//	if p.err = s.handle(&p); p.err != nil {
		//		break
		//	}
		//
		//	// Added for posterity, however Ack is a no-op.
		//	if s.opts.AutoAck {
		//		if err := p.Ack(); err != nil {
		//			break
		//		}
		//	}
		//
		//case redis.Subscription:
		//	fmt.Println("subscription type")
		//	if x.Count == 0 {
		//		return
		//	}
		//
		//case error:
		//	return
		//}
	//}
}

// Options returns the subscriber options.
func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

// Topic returns the topic of the subscriber.
func (s *subscriber) Topic() string {
	return s.topic
}

// Unsubscribe unsubscribes the subscriber and frees the connection.
func (s *subscriber) Unsubscribe() error {
	// TODO to refractor this later
	s.client.Do("unsubscribe")
	//s.client.Conn().Close()
	return nil
}

// broker implementation for Redis.
type redisBroker struct {
	addr  string
	client  *redis.Client
	opts  broker.Options
	bopts *brokerOptions
}

// String returns the name of the broker implementation.
func (b *redisBroker) String() string {
	return "redis"
}

// Options returns the options defined for the broker.
func (b *redisBroker) Options() broker.Options {
	return b.opts
}

// Address returns the address the broker will use to create new connections.
// This will be set only after Connect is called.
func (b *redisBroker) Address() string {
	return b.addr
}

// Init sets or overrides broker options.
func (b *redisBroker) Init(opts ...broker.Option) error {
	if b.client != nil {
		return errors.New("redis: cannot init while connected")
	}

	for _, o := range opts {
		o(&b.opts)
	}

	return nil
}

// Connect establishes a connection to Redis which provides the
// pub/sub implementation.
func (b *redisBroker) Connect() error {
	if b.client != nil {
		return nil
	}

	var addr string

	if len(b.opts.Addrs) == 0 || b.opts.Addrs[0] == "" {
		addr = "127.0.0.1:6379"
	} else {
		addr = b.opts.Addrs[0]

		//if !strings.HasPrefix("redis://", addr) {
		//	addr = "redis://" + addr
		//}
	}

	b.addr = addr

	//b.pool = &redis.Pool{
	//	MaxIdle:     b.bopts.maxIdle,
	//	MaxActive:   b.bopts.maxActive,
	//	IdleTimeout: b.bopts.idleTimeout,
	//	Dial: func() (redis.Conn, error) {
	//		return redis.DialURL(
	//			b.addr,
	//			redis.DialConnectTimeout(b.bopts.connectTimeout),
	//			redis.DialReadTimeout(b.bopts.readTimeout),
	//			redis.DialWriteTimeout(b.bopts.writeTimeout),
	//		)
	//	},
	//	TestOnBorrow: func(c redis.Conn, t time.Time) error {
	//		_, err := c.Do("PING")
	//		return err
	//	},
	//}
	b.client = redis.NewClient(
		&redis.Options{
			Network: "",
			Addr:				b.addr,
			Dialer:             nil,
			OnConnect:          nil,
			Password:           "",
			DB:                 0,
			PoolSize:			10,

		})
	return nil
}

// Disconnect closes the connection pool.
func (b *redisBroker) Disconnect() error {
	err := b.client.Conn().Close()
	b.client = nil
	b.addr = ""
	return err
}

// Publish publishes a message.
func (b *redisBroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	v, err := b.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}

	//cmd := b.client.Do("PUBLISH", topic, v)
	//_, err = redis.Int(b.client.Do("PUBLISH", topic, v))
	//fmt.Println(b.Address())
	cmd := b.client.Publish(topic, v)
	if cmd == nil {
		fmt.Println("cmd is nil")
	}
	b.client.Conn().Close()

	return err
}

// Subscribe returns a subscriber for the topic and handler.
func (b *redisBroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	var options broker.SubscribeOptions
	for _, o := range opts {
		o(&options)
	}

	s := subscriber{
		codec:  b.opts.Codec,
		client:  b.client,
		topic:  topic,
		handle: handler,
		opts:   options,
	}

	// Run the receiver routine.
	go s.recv(s.topic)

	if pubsub := s.client.Subscribe(s.topic); pubsub == nil {
		// TODO add error for this later
		return nil, nil
	}

	return &s, nil
}

// NewBroker returns a new broker implemented using the Redis pub/sub
// protocol. The connection address may be a fully qualified IANA address such
// as: redis://user:secret@localhost:6379/0?foo=bar&qux=baz
func NewBroker(opts ...broker.Option) broker.Broker {
	// Default options.
	bopts := &brokerOptions{
		maxIdle:        DefaultMaxIdle,
		maxActive:      DefaultMaxActive,
		idleTimeout:    DefaultIdleTimeout,
		connectTimeout: DefaultConnectTimeout,
		readTimeout:    DefaultReadTimeout,
		writeTimeout:   DefaultWriteTimeout,
	}

	// Initialize with empty broker options.
	options := broker.Options{
		Codec:   json.Marshaler{},
		Context: context.WithValue(context.Background(), optionsKey, bopts),
	}

	for _, o := range opts {
		o(&options)
	}

	return &redisBroker{
		opts:  options,
		bopts: bopts,
	}
}


//func Int(reply interface{}, err error) (int, error) {
//	if err != nil {
//		return 0, err
//	}
//	switch reply := reply.(type) {
//	case int64:
//		x := int(reply)
//		if int64(x) != reply {
//			return 0, strconv.ErrRange
//		}
//		return x, nil
//	case []byte:
//		n, err := strconv.ParseInt(string(reply), 10, 0)
//		return int(n), err
//	case nil:
//		return 0, ErrNil
//	case Error:
//		return 0, reply
//	}
//	return 0, fmt.Errorf("redigo: unexpected type for Int, got type %T", reply)
//}
