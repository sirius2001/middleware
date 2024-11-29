package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"github.com/sirius2001/middleware/sarama_storer"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	brokers := []string{"localhost:9091"}

	// 连接到 SQLite 数据库
	db, err := gorm.Open(sqlite.Open("./test.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}
	_ = db
	//实现storer
	//storer := sarama_storer.NewGormStorer(db)

	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})

	storer := sarama_storer.NewRDBStorer(rdb)

	// 配置生产者
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 等待所有副本都确认
	config.Producer.Retry.Max = 5                             // 最大重试次数
	config.Producer.Return.Successes = true                   // 确认发送成功
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 随机分区
	config.Consumer.Interceptors = append(config.Consumer.Interceptors, sarama_storer.NewDefalutConsumeInterceptor(storer))
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}

	//查找storer中最后的offset
	offset := storer.Offset("example-topic", 0)

	pc, err := consumer.ConsumePartition("example-topic", 0, offset)
	if err != nil {
		panic(err)
	}

	for range 3 {
		go func() {
			for m := range pc.Messages() {
				fmt.Println(m)
			}
		}()
	}

	time.Sleep(time.Second)
}
