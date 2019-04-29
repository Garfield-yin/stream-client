package main

import (
	"fmt"
	"stream-client/nsq"
	"time"
)

func main() {
	consumer, err := nsq.NewConsumer("127.0.0.1:4161", "127.0.0.1:4150", "test_topic", "channel")
	if err != nil {
		fmt.Println("New nsq consumer error:", err)
		return
	}
	consumer.Subscribe()
	// recv message
	go func() {
		for {
			msg, _ := consumer.Recv()
			fmt.Println("recv message:", string(msg))
		}
	}()

	// producer
	producer, err := nsq.NewProducer("127.0.0.1:4150")
	if err != nil {
		fmt.Println("New nsq  producer error:", err)
		return
	}

	defer producer.Destroy()
	defer consumer.Destroy()

	// send message
	sendCount := 0
	ticker := time.NewTicker(time.Second * 1)
	for _ = range ticker.C {
		msgStr := fmt.Sprintf("%d", sendCount)
		// send message
		producer.Send("test_topic", []byte(msgStr), time.Second*3, true)
		sendCount++
		fmt.Printf("send message count: %v,%v\n", sendCount, time.Now())
	}
}
