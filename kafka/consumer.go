package kafka

import (
	"errors"
	"fmt"

	"stream-client/common/logger"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

/**
有两种方式消费：
1.处理完一条消息，手动commit 0ffset
2.消息入库之后，记录offset值，每次启动读取offset,从上一次的数据开始消费
第1中方式简单高效，但存在消息丢失或重复消费的可能
第2种方式完全可靠
采用哪种方式由调用者控制
*/
type Consumer struct {
	consumer *cluster.Consumer
	errors   chan error
	done     chan bool
}

func NewConsumer(brokers []string, topics []string, group string, config *cluster.Config) (*Consumer, error) {
	logger.Info.Println("kafka consumer brokers:", brokers)
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	consumer, err := cluster.NewConsumer(brokers, group, topics, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer: consumer,
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}, nil
}

func (k *Consumer) Subscribe() error {
	go func() {
		for ntf := range k.consumer.Notifications() {
			fmt.Printf("Rebalanced: %+v\n", ntf)
		}
	}()
	go func() {
		for err := range k.consumer.Errors() {
			logger.Error.Printf("Error: %s\n", err.Error())
			k.errors <- err
		}
	}()
	return nil
}

func (k *Consumer) Recv() (*sarama.ConsumerMessage, error) {
	msg, ok := <-k.consumer.Messages()
	if ok {
		return msg, nil
	}
	return msg, errors.New("null message")
}

func (k *Consumer) MarkOffset(msg *sarama.ConsumerMessage) {
	k.consumer.MarkOffset(msg, "")
}

func (k *Consumer) Errors() <-chan error {
	return k.errors
}

func (k *Consumer) Done() {
	k.done <- true
}

func (k *Consumer) Destroy() {
	k.consumer.Close()
}
