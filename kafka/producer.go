package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type Producer struct {
	producer sarama.AsyncProducer
	send     chan []byte
	errors   chan error
	done     chan bool
}

func NewProducer(host string, port uint64) (*Producer, error) {
	addRess := fmt.Sprintf("%s:%d", host, port)
	config := sarama.NewConfig()

	producer, err := sarama.NewAsyncProducer([]string{addRess}, config)
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer: producer,
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}, nil
}

func (k *Producer) Send() chan<- []byte {
	return k.send
}

func (k *Producer) Errors() <-chan error {
	return k.errors
}

func (k *Producer) Done() {
	k.done <- true
}

func (k *Producer) Setup(topic string) {
	go func() {
		for {
			select {
			case msg := <-k.send:
				if err := k.sendMessage(topic, msg); err != nil {
					k.errors <- err
				}
			case <-k.done:
				return
			}
		}
	}()
}

func (k *Producer) sendMessage(topic string, message []byte) error {
	select {
	case k.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(message)}:
		return nil
	case err := <-k.producer.Errors():
		return err.Err
	}
}

func (k *Producer) Destroy() {
	k.producer.Close()
}
