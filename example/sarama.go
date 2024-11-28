package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirius2001/middleware/sarama_storer"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	brokers := []string{"localhost:9091"}

	// 配置生产者
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 等待所有副本都确认
	config.Producer.Retry.Max = 5                             // 最大重试次数
	config.Producer.Return.Successes = true                   // 确认发送成功
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 随机分区

	// 连接到 SQLite 数据库
	db, err := gorm.Open(sqlite.Open("./test.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	// 创建 Kafka 生产者
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}
	defer producer.Close()

	// 创建中间件存储实例
	storer, err := sarama_storer.NewSyncProducerStore(sarama_storer.NewDBStorer(db), producer)
	if err != nil {
		log.Fatalf("Failed to create producer store: %v", err)
	}

	// 创建要发送的消息
	message := &sarama.ProducerMessage{
		Topic:     "example-topic",
		Value:     sarama.StringEncoder("Hello, Kafka!"),
		Partition: 0, // 使用任何分区
	}

	// // 向 Kafka 发送消息
	// producer.Input() <- message

	if err := storer.SendMessages([]*sarama.ProducerMessage{message}); err != nil {
		panic(err)
	}

	//等待数据入库
	time.Sleep(1 * time.Second)

	offset, err := storer.Offset("example-topic", 0)
	if err != nil {
		panic(err)
	}
	fmt.Println(offset)
}
