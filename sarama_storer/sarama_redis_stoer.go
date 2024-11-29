package sarama_storer

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

type RDBStorer struct {
	rdb *redis.Client
}

// Init implements Storer.
func (r *RDBStorer) Init() error {
	if r.rdb == nil {
		return errors.New("rdb is nil")
	}
	if err := r.rdb.Ping(context.Background()).Err(); err != nil {
		return err
	}
	return nil
}

// Offset implements Storer.
func (r *RDBStorer) Offset(topic string, partition int32) int64 {
	offsetStr, err := r.rdb.HGet(context.Background(), "storer-rdb", "offset").Result()
	if err != nil {
		return 0
	}
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return 0
	}
	return int64(offset)
}

// Save implements Storer.
func (r *RDBStorer) Save(msg *sarama.ConsumerMessage) error {
	m := &MessageRecord{
		ID:        fmt.Sprintf("%s-%d", msg.Topic, msg.Partition),
		Offset:    msg.Offset,
		Partition: msg.Partition,
		Topic:     msg.Topic,
		TimeStamp: msg.Timestamp,
	}

	return r.rdb.HSet(context.Background(), "storer-rdb", m).Err()
}

func NewRDBStorer(rdb *redis.Client) Storer {
	return &RDBStorer{
		rdb: rdb,
	}
}
