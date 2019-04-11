package main

import (
	"fmt"
	"stream-client/nsq"
	"time"
)

func main() {
	consumer, err := nsq.NewConsumer("192.168.4.130:4161", "192.168.4.130:4150", "test_topic", "channel")
	if err != nil {
		fmt.Println("New nsq consumer error:", err)
		return
	}
	consumer.Subscribe()
	// recv message
	go func() {
		for {
			msg, _ := consumer.Recv()
			fmt.Println("recv message:", msg)
		}
	}()

	// producer
	producer, err := nsq.NewProducer("192.168.4.130:4150")
	if err != nil {
		fmt.Println("New nsq  producer error:", err)
		return
	}
	producer.Setup("test_topic")
	defer producer.Destroy()
	defer consumer.Destroy()

	// send message,仅支持PublishAsync,后期改善
	sendCount := 0
	ticker := time.NewTicker(time.Second * 5)
	for _ = range ticker.C {
		msgStr := fmt.Sprintf("%d", sendCount)
		// send message
		producer.Send() <- []byte(msgStr)
		sendCount++
		fmt.Printf("send message count: %v,%v\n", sendCount, time.Now())
	}
}
