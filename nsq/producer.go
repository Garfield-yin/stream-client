package nsq

import (
	"time"

	nsqclient "github.com/nsqio/go-nsq"
)

type producerConfig struct {
	NsqdTCPAddrs string
	Delay        time.Duration // 单位/s
}

type Producer struct {
	producer *nsqclient.Producer
	messages chan []byte
	errors   chan error
	done     chan bool
	flush    chan bool
}

func NewProducer(nsqdAddress string) (*Producer, error) {
	producer, err := nsqclient.NewProducer(nsqdAddress, nsqclient.NewConfig())
	if err != nil {
		return nil, err
	}
	return &Producer{
		producer: producer,
		messages: make(chan []byte, 100),
		errors:   make(chan error, 1),
		done:     make(chan bool),
		flush:    make(chan bool),
	}, nil
}

func (stream *Producer) Setup(topic string) {
	go func() {
		for {
			select {
			case msg := <-stream.messages:
				if err := stream.producer.PublishAsync(topic, msg, nil); err != nil {
					stream.errors <- err
				}
			case <-stream.done:
				stream.flush <- true
				return
			}
		}
	}()
}

// Send returns a channel on which messages can be sent for publishing.
func (stream *Producer) Send() chan<- []byte {
	return stream.messages
}

// Errors returns the channel on which the peer sends publish errors.
func (stream *Producer) Errors() <-chan error {
	return stream.errors
}

// Done signals to the peer that message publishing has completed.
func (stream *Producer) Done() {
	stream.done <- true
	<-stream.flush
}

func (stream *Producer) Destroy() {
	stream.producer.Stop()
}
