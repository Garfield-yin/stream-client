package nsq

import (
	"errors"
	"fmt"
	"log"
	"os"

	nsqclient "github.com/nsqio/go-nsq"
)

type ConsumerConfig struct {
	NsqdTCPAddrs     string
	LookupdHTTPAddrs string
	Topic            string
	Channel          string
}

type Consumer struct {
	consumer *nsqclient.Consumer
	messages chan []byte
	config   ConsumerConfig
	errors   chan error
	done     chan bool
	flush    chan bool
}

func NewConsumer(lookupdAddress, nsqdTcpAddress, topic, channel string) (*Consumer, error) {
	config := ConsumerConfig{
		NsqdTCPAddrs:     nsqdTcpAddress,
		LookupdHTTPAddrs: lookupdAddress,
		Topic:            topic,
		Channel:          channel,
	}
	return &Consumer{
		messages: make(chan []byte, 10000),
		errors:   make(chan error, 1),
		config:   config,
		done:     make(chan bool),
		flush:    make(chan bool),
	}, nil
}

func RegisterConsumer(config ConsumerConfig, handlers ...nsqclient.HandlerFunc) (*nsqclient.Consumer, error) {
	cfg := nsqclient.NewConfig()

	consumer, err := nsqclient.NewConsumer(config.Topic, config.Channel, cfg)
	if err != nil {
		fmt.Println("err:", err)
		return nil, err
	}
	consumer.SetLogger(log.New(os.Stderr, "", log.Flags()), nsqclient.LogLevelWarning)

	for _, handler := range handlers {
		consumer.AddHandler(handler)
	}

	if config.LookupdHTTPAddrs != "" {
		err = consumer.ConnectToNSQLookupd(config.LookupdHTTPAddrs)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
	} else if config.NsqdTCPAddrs != "" {
		err = consumer.ConnectToNSQD(config.NsqdTCPAddrs)
		if err != nil {
			fmt.Println(err)
			return nil, err

		}
	} else {
		return nil, errors.New("error nsq config")
	}
	return consumer, nil
}

func (stream *Consumer) Subscribe() error {
	RegisterConsumer(stream.config, func(message *nsqclient.Message) error {
		stream.messages <- message.Body
		return nil
	})
	return nil
}

func (stream *Consumer) Recv() ([]byte, error) {
	return <-stream.messages, nil
}

func (stream *Consumer) Errors() <-chan error {
	return stream.errors
}

func (stream *Consumer) Done() {
	stream.done <- true
	<-stream.flush
}

func (stream *Consumer) Destroy() {
	if stream.consumer != nil {
		stream.consumer.Stop()
		<-stream.consumer.StopChan
	}
}
