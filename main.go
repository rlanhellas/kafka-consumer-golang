package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	config := &kafka.ConfigMap{"bootstrap.servers": "localhost", "group.id": "ronaldo-v1", "enable.auto.commit": false,
		"partition.assignment.strategy": "cooperative-sticky", "auto.offset.reset": "earliest"}

	c, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}

	err = c.SubscribeTopics([]string{"test"}, nil)
	if err != nil {
		panic(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	seekTime := 0
	for {
		select {
		case <-sig:
			if !c.IsClosed() {
				err = c.Close()
				if err != nil {
					panic(err)
				}
				log.Println("consumer stopped")
				break
			}
		default:
			if !c.IsClosed() {
				msg, err := c.ReadMessage(time.Second)
				if err == nil {
					log.Printf("key=%v,value=%v,partition=%d,offset=%d", string(msg.Key), string(msg.Value),
						msg.TopicPartition.Partition, msg.TopicPartition.Offset)
					if strings.Contains(string(msg.Value), "seek") && seekTime < 10 {
						seekTime++
						_, err = c.SeekPartitions([]kafka.TopicPartition{msg.TopicPartition})
						if err != nil {
							panic(err)
						}
						time.Sleep(time.Second * 2)
					} else {
						seekTime = 0
						_, err = c.CommitMessage(msg)
						if err != nil {
							panic(err)
						}
					}
				} else if !err.(kafka.Error).IsTimeout() {
					log.Printf("Consumer error: %v (%v)\n", err, msg)
				}
			}
		}

		if c.IsClosed() {
			break
		}
	}

}
