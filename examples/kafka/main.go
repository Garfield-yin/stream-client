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

	// sync producer
	syncConfig := sarama.NewConfig()
	syncConfig.Producer.Retry.Max = 5
	syncConfig.Producer.RequiredAcks = sarama.WaitForAll
	syncConfig.Producer.Retry.Backoff = time.Minute * 3
	syncConfig.Producer.Return.Successes = true
	syncProducer, err := kafka.NewSyncProducer([]string{"127.0.0.1:9092"}, syncConfig)
	if err != nil {
		fmt.Println("New kafka sync producer error:", err)
		return
	}

	// async producer
	asyncConfig := sarama.NewConfig()
	asyncConfig.Producer.Retry.Max = 5
	//等待服务器所有副本都保存成功后的响应
	asyncConfig.Producer.RequiredAcks = sarama.WaitForAll
	asyncConfig.Producer.Retry.Backoff = time.Minute * 3
	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
	asyncConfig.Producer.Return.Successes = true
	asyncConfig.Producer.Return.Errors = true
	asyncProducer, err := kafka.NewAsyncProducer([]string{"127.0.0.1:9092"}, asyncConfig)
	if err != nil {
		fmt.Println("New kafka async producer error:", err)
		return
	}
	// return.Successes,return.Errors 为true 需要处理error 和 succedss,否则会阻塞
	go func() {
		for {
			select {
			case suc := <-asyncProducer.Successes():
				fmt.Println("async publish offset: ", suc.Offset, "timestamp: ", suc.Timestamp.String(), "partitions: ", suc.Partition)
			case fail := <-asyncProducer.Errors():
				if fail != nil {
					fmt.Println("async send error:", fail.Error())
				}
			}
		}
	}()

	defer syncProducer.Destroy()
	defer asyncProducer.Destroy()
	defer consumer.Destroy()
	ticker := time.NewTicker(time.Second * 5)
	sendCount := 0
	for _ = range ticker.C {
		msgStr := fmt.Sprintf("%d", sendCount)

		// sync send message
		_, _, err := syncProducer.Send("topic_test", []byte(msgStr))
		if err != nil {
			fmt.Println("send message failed,", err)
			continue
		}

		// async send message
		asyncProducer.Send("topic_test", []byte(msgStr))
		sendCount++
		fmt.Printf("send message count: %v,%v\n", sendCount, time.Now())
	}
}
