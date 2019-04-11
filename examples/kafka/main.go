package main

import (
	"fmt"
	"stream-client/kafka"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func main() {
	// consumer

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	consumer, err := kafka.NewConsumer([]string{"127.0.0.1:9092"}, []string{"topic_test"}, "test_group", config)
	if err != nil {
		fmt.Println("New kafka consumer error:", err)
		return
	}
	consumer.Subscribe()
	// recv message
	go func() {
		for {
			msg, _ := consumer.Recv()
			fmt.Println("recv message:", string(msg.Value))
			consumer.MarkOffset(msg)
		}
	}()

	// producer
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Retry.Max = 5
	producerConfig.Producer.Retry.Backoff = time.Minute * 3
	producer, err := kafka.NewProducer([]string{"127.0.0.1:9092"}, producerConfig)
	if err != nil {
		fmt.Println("New kafka producer error:", err)
		return
	}
	producer.Setup("topic_test")
	defer producer.Destroy()
	defer consumer.Destroy()
	ticker := time.NewTicker(time.Second * 5)
	sendCount := 0
	for _ = range ticker.C {
		msgStr := fmt.Sprintf("%d", sendCount)
		// send message
		producer.Send() <- []byte(msgStr)
		sendCount++
		fmt.Printf("send message count: %v,%v\n", sendCount, time.Now())
	}
}
