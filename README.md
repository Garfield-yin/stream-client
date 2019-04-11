# stream-client
stream-client is an simple Go client api library for Kafka and nsq.

## Installation
1. Download and install it:

```sh
$ go get -u github.com/garfield-yin/stream-client
```

2. Import it in your code:

```go
import "github.com/garfield-yin/stream-client/kafka"
import "github.com/garfield-yin/stream-client/nsq"
```

## Quick start
Currently the function is relatively simple.

### Kafka
// 后期会采用接口形式封装
```go
package main

import (
	"fmt"
	"stream-client/kafka"
	"github.com/Shopify/sarama"
)

func main() {
	// consumer
	consumer, err := kafka.NewConsumer([]string{"127.0.0.1:9092"}, []string{"topic_1"}, "group")
	if err != nil {
		fmt.Println("New kafka consumer error:", err)
		return
	}
	consumer.Subscribe()
	// recv message
	for {
		msg, _ := consumer.Recv()
		consumer.MarkOffset(msg)
	}

	// producer
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = time.Minute*3
	producer, err := kafka.NewProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("New kafka producer error:", err)
		return
	}
	producer.Setup("topic")
	// send message
	producer.Send() <- []byte("message")
}

```

### nsq
```go
    // 接收到消息自动finish，后期改善支持手动
	// consumer
	consumer, err := nsq.NewConsumer("127.0.0.1:4161", "127.0.0.1:4150", "topic", "channel")
	if err != nil {
		fmt.Println("New nsq consumer error:", err)
		return
	}
	consumer.Subscribe()
	// recv message
	for {
		msg, _ := consumer.Recv()
		fmt.Println("message:", msg)
	}

	// producer
	producer, err := nsq.NewProducer("127.0.0.1:4150")
	if err != nil {
		fmt.Println("New nsq  producer error:", err)
		return
	}
	producer.Setup("topic")
	// send message,仅支持PublishAsync,后期改善
	producer.Send() <- []byte("message")

```