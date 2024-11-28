package sarama_storer

import (
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type ProducerRecord struct {
	ID        string    `json:"id" gorm:"column:id;primarykey"`      // 主键 ID
	Offset    int64     `json:"offset" gorm:"column:offset"`         // 消息偏移量
	Partition int32     `json:"partition" gorm:"column:partition"`   // 分区号
	Topic     string    `json:"topic" gorm:"column:topic;index"`     // 主题（索引）
	TimeStamp time.Time `json:"time_stamp" gorm:"column:time_stamp"` // 时间戳
}

type AsyncProducerStorer struct {
	storer   Storer
	records  chan *ProducerRecord
	producer sarama.AsyncProducer
}

// conf.Consumer.Interceptors = append(conf.Consumer.Interceptors, )
func NewAsyncProducerStore(storer Storer, producer sarama.AsyncProducer) (*AsyncProducerStorer, error) {
	if storer == nil {
		return nil, errors.New("db is nil,system error")
	}

	if producer == nil {
		return nil, errors.New("producer is nil,system error")
	}

	if err := storer.Init(); err != nil {
		return nil, err
	}

	p := &AsyncProducerStorer{
		storer:   storer,
		records:  make(chan *ProducerRecord, 1024),
		producer: producer,
	}

	go p.on()
	go p.revLoop()

	return p, nil
}

func (d *AsyncProducerStorer) on() {
	for msg := range d.producer.Successes() {
		p := &ProducerRecord{
			ID:        fmt.Sprintf("%s-%d", msg.Topic, msg.Partition),
			Offset:    msg.Offset,
			Partition: msg.Partition,
			Topic:     msg.Topic,
			TimeStamp: msg.Timestamp,
		}

		select {
		case d.records <- p:
			return
		default:
			if err := d.storer.Save(p); err != nil {
				panic(err)
			}
		}
	}
}

func (d *AsyncProducerStorer) Offset(topic string, partition int32) (int64, error) {
	p, err := d.storer.Read(topic, partition)
	if err != nil || p == nil {
		return 0, err
	}
	return p.Offset, err
}

func (d *AsyncProducerStorer) revLoop() {
	for r := range d.records {
		if err := d.storer.Save(r); err != nil {
			continue
		}
	}
}
