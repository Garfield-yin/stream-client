package kafka

import (
	"github.com/Shopify/sarama"
)

type AsyncProducer struct {
	producer  sarama.AsyncProducer
	errors    chan error
	successes chan *sarama.ProducerMessage
}

func NewAsyncProducer(brokers []string, config *sarama.Config) (*AsyncProducer, error) {
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	asyncProducer := &AsyncProducer{
		producer:  producer,
		errors:    make(chan error, 1),
		successes: make(chan *sarama.ProducerMessage, 1),
	}

	go func() {
		for {
			select {
			case err := <-asyncProducer.producer.Errors():
				asyncProducer.errors <- err
			case suc := <-asyncProducer.producer.Successes():
				asyncProducer.successes <- suc
			}
		}
	}()
	return asyncProducer, nil
}

func (k *AsyncProducer) Errors() <-chan error {
	return k.errors
}

func (k *AsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return k.successes
}

func (k *AsyncProducer) Send(topic string, message []byte) {
	k.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(message)}
}

func (k *AsyncProducer) SendRawMsg(msg *sarama.ProducerMessage) {
	k.producer.Input() <- msg
}

func (k *AsyncProducer) Destroy() {
	k.producer.Close()
}
