package sarama_store

import (
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"gorm.io/gorm"
)

type ProducerRecord struct {
	ID        string    `json:"id" gorm:"column:id;primarykey"`      // 主键 ID
	Offset    int64     `json:"offset" gorm:"column:offset"`         // 消息偏移量
	Partition int32     `json:"partition" gorm:"column:partition"`   // 分区号
	Topic     string    `json:"topic" gorm:"column:topic;index"`     // 主题（索引）
	TimeStamp time.Time `json:"time_stamp" gorm:"column:time_stamp"` // 时间戳
}

type DefaultProducerInterceptor struct {
	db      *gorm.DB
	records chan *ProducerRecord
}

// conf.Consumer.Interceptors = append(conf.Consumer.Interceptors, )
func NewDefaultProducerInterceptor(db *gorm.DB) (*DefaultProducerInterceptor, error) {
	if db == nil {
		return nil, errors.New("db is nil,system error")
	}

	if err := db.AutoMigrate(); err != nil {
		return nil, err
	}

	p := &DefaultProducerInterceptor{
		db:      db,
		records: make(chan *ProducerRecord, 1024),
	}

	go p.revLoop()

	return p, nil
}

func (d *DefaultProducerInterceptor) OnSend(msg *sarama.ProducerMessage) {
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
		d.db.Model(p).Save(p)
	}
}

func (d *DefaultProducerInterceptor) Offset(topic string, partition int32) (int64, error) {
	p := new(ProducerRecord)
	if err := d.db.Model(p).Where("id = ?", fmt.Sprintf("%s-%d", topic, partition)).First(&p).Error; err != nil {
		return 0, err
	}
	return p.Offset, nil
}

func (d *DefaultProducerInterceptor) revLoop() {
	for r := range d.records {
		if err := d.db.Model(r).Save(r).Error; err != nil {
			continue
		}
	}
}
