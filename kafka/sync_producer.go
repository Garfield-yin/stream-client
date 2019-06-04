package kafka

import (
	"github.com/Shopify/sarama"
)

type SyncProducer struct {
	producer sarama.SyncProducer
}

func NewSyncProducer(brokers []string, config *sarama.Config) (*SyncProducer, error) {
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &SyncProducer{
		producer: producer,
	}, nil
}

func (k *SyncProducer) Send(topic string, message []byte) (partition int32, offset int64, err error) {
	msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(message)}
	pid, offset, err := k.producer.SendMessage(msg)
	return pid, offset, err
}

func (k *SyncProducer) SendRawMsg(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	pid, offset, err := k.producer.SendMessage(msg)
	return pid, offset, err
}

func (k *SyncProducer) SendRawMsgs(msgs []*sarama.ProducerMessage) (err error) {
	err = k.producer.SendMessages(msgs)
	return err
}

func (k *SyncProducer) Destroy() {
	k.producer.Close()
}
