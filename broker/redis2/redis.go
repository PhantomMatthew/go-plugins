// Package redis provides a Redis broker
package redis2

import "C"
import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
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
	consumerGroup string
	stream string
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
	client *redis.Client
	consumerGroup string
	stream string
	topic  string
	handle broker.Handler
	opts   broker.SubscribeOptions
}

func (s *subscriber) deleteLastMessages(receiverStreamName string, groupName string, id string) {
	// 查看这个stream的消费组状态
	xinfoCmd := s.client.Pipeline().XInfoGroups(receiverStreamName)
	xinfoGroups, err := xinfoCmd.Result()

	if err == nil && len(xinfoGroups) > 0 {
		// 获得排除当前组后的group组
		var otherGroups []redis.XInfoGroups
		for _, item := range xinfoGroups {
			if item.Name != groupName {
				otherGroups = append(otherGroups, item)
			}
		}
		if len(otherGroups) > 0 {
			// 判断当前消费组接收ID小于其他分组的lastid就删除
			minId, _ := strconv.Atoi(strings.Split(otherGroups[0].LastDeliveredID, "-")[0])
			for i := 1; i < len(otherGroups); i++ {
				newValue, _ := strconv.Atoi(strings.Split(otherGroups[i].LastDeliveredID, "-")[0])
				if minId > newValue {
					minId = newValue
				}
			}
			currentId, _ := strconv.Atoi(strings.Split(id, "-")[0])
			if currentId < minId {
				//当前组为最后消费的组
				delResult := s.client.Pipeline().XDel(receiverStreamName, id)
				fmt.Println(delResult)
			}
		}

	}
}

// recv loops to receive new messages from Redis and handle them
// as publications.
func (s *subscriber) recv(stream string, consumerGroup string, consumer string) {

	// Close the connection once the subscriber stops receiving.
	//defer s.client.Conn().Close()
	defer s.client.Pipeline().Close()


	check_backlog := true
	var lastId = "0-0"

	for {
		log.Println("in first for loop of recv")
		var myId string
		if check_backlog {
			myId = lastId
		} else {
			myId = ">"
		}

		args := redis.XReadGroupArgs{
			Group:    consumerGroup,
			Consumer: "customer",
			Streams:  []string{stream, myId},
			Count:    10,
			Block:    time.Duration(5) * time.Second,
		}

		result := s.client.XReadGroup(&args)

		ss := result.Val()
		if ss == nil {
			fmt.Println("no message")
			//time.Sleep(time.Second * 2)
			continue
		}

		// TODO to update the processing later
		// 说明已经处理完了之前 未确认 的消息，开始处理新消息，lastId改为 >
		if len(ss[0].Messages) == 0 {
			fmt.Println("read end")
			check_backlog = false
		}

		for _, item := range ss {
			messages := item.Messages

			log.Println("in second for loop for messages iterations")

			for _, msg := range messages {
				id := msg.ID
				values := msg.Values
				log.Println("result:",id, values)
				if values["yz"] != nil {
					log.Println("values stream is not nil")

					tempMessage := values["yz"].(string)
					log.Println("message is " + tempMessage)
					p := publication{
						topic:   s.stream,
						message: &broker.Message{
							Header: map[string]string{
								s.stream: stream,
							},
							Body:   []byte(tempMessage),
						},
					}

					// Handle error? Retry?
					if p.err = s.handle(&p); p.err != nil {
						log.Fatal("handle message error")
					} else {
						// 消息消费确认
						s.client.Pipeline().XAck(stream, consumerGroup, id)
						// 验证是否消费组的所有人都消费后的逻辑
						s.deleteLastMessages(stream, consumerGroup, id)

						lastId = id
						//time.Sleep(time.Second * 0)
					}
				}


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

func (s *subscriber) Stream() string {
	return s.stream
}

// Unsubscribe unsubscribes the subscriber and frees the connection.
func (s *subscriber) Unsubscribe() error {
	// TODO to refractor this later
	//s.client.Do("unsubscribe", s.Topic())
	s.client.Pipeline().Close()
	return nil
}

// broker implementation for Redis.
type redisBroker struct {
	addr  string
	client  *redis.Client
	consumerGroup string
	opts  broker.Options
	bopts *brokerOptions
}

func (b *redisBroker) SetConsumerGroup(consumerGroup string) string {
	b.consumerGroup = consumerGroup
	return b.consumerGroup
}

func (b *redisBroker) ConsumerGroup() string {
	return b.consumerGroup
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
	log.Println(b.addr)
	addresses := strings.Split(b.addr, "@")
	clientAddress := addresses[1]
	log.Println("client address is", clientAddress)
	clientPassword := addresses[0]
	log.Println("client password is", clientPassword)

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
			Addr:				clientAddress,
			Dialer:             nil,
			OnConnect:          nil,
			Password:           clientPassword,
			DB:                 0,
			PoolSize:			10,

		})
	return nil
}

// Disconnect closes the connection pool.
func (b *redisBroker) Disconnect() error {
	err := b.client.Pipeline().Close()
	b.client = nil
	b.addr = ""
	return err
}

func (b *redisBroker) publish(stream, consumerGroup string, values map[string]interface{}) string {
	exists := b.client.Exists(stream)
	if exists.Val() > 0 {
		id := b.client.XAdd(&redis.XAddArgs{
			Stream:       "",
			MaxLen:       0,
			MaxLenApprox: 0,
			ID:           "*",
			Values:       nil,
		}).Val()

		b.client.XDel(stream, id)
	}


	result := b.client.XAdd(&redis.XAddArgs{
		Stream:       stream,
		MaxLen:       5 * 100000,
		MaxLenApprox: 0,
		ID:           "*",
		Values:       values,

	})

	return result.Val()

}

// Publish publishes a message.
func (b *redisBroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {

	var values map[string]interface{}
	values = make(map[string]interface{})
	values[topic] = string(msg.Body[:])

	b.publish(topic, b.ConsumerGroup(), values)


	err := b.client.Pipeline().Close()

	return err
}

// Subscribe returns a subscriber for the topic and handler.
func (b *redisBroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	log.Println("redis subscribe")
	var options broker.SubscribeOptions
	for _, o := range opts {
		o(&options)
	}
	log.Println("consumer group is", b.consumerGroup)
	//b.consumerGroup = "dzzs"

	s := subscriber{
		codec:         b.opts.Codec,
		client:        b.client,
		consumerGroup: b.ConsumerGroup(),
		stream:        topic,
		topic:         topic,
		handle:        handler,
		opts:          options,
	}
	s.client.XGroupCreate(s.stream, b.consumerGroup, "$")

	go s.recv(topic, s.consumerGroup, "consumer")

	// Run the receiver routine.
	//go s.recv(topic)

	//if pubsub := s.client.Subscribe(s.topic); pubsub == nil {
	//	// TODO add error for this later
	//	return nil, nil
	//}

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

