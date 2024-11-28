```go
brokers := []string{"localhost:9091"}

	// 连接到 SQLite 数据库
	db, err := gorm.Open(sqlite.Open("./test.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	//实现storer
	storer := sarama_storer.NewDBStorer(db)

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

	for m := range pc.Messages() {
		fmt.Println(m)
	}
```

