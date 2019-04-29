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
}

func NewProducer(nsqdAddress string) (*Producer, error) {
	producer, err := nsqclient.NewProducer(nsqdAddress, nsqclient.NewConfig())
	if err != nil {
		return nil, err
	}
	return &Producer{
		producer: producer,
	}, nil
}

// Setup 启动发送
/*
@delay 是否延迟推送，<=0 不延迟
@isAsync 是否异步发送
*/
func (stream *Producer) Send(topic string, msg []byte, delay time.Duration, isAsync bool) error {
	var err error
	if delay > 0 { // 延迟发送
		if isAsync {
			err = stream.producer.DeferredPublishAsync(topic, delay, msg, nil, nil)
		} else {
			err = stream.producer.DeferredPublish(topic, delay, msg)
		}
	} else { // 立即发送
		if isAsync {
			err = stream.producer.PublishAsync(topic, msg, nil)
		} else {
			err = stream.producer.Publish(topic, msg)
		}
	}
	return err
}

func (stream *Producer) Destroy() {
	stream.producer.Stop()
}
