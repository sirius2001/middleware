package sarama_storer

import (
	"time"

	"github.com/IBM/sarama"
)

type MessageRecord struct {
	ID        string    `json:"id" gorm:"column:id;primarykey" redis:"ID"`              // 主键 ID
	Offset    int64     `json:"offset" gorm:"column:offset" redis:"offset"`             // 消息偏移量
	Partition int32     `json:"partition" gorm:"column:partition" redis:"partition"`    // 分区号
	Topic     string    `json:"topic" gorm:"column:topic;index" redis:"topic"`          // 主题（索引）
	TimeStamp time.Time `json:"time_stamp" gorm:"column:time_stamp" redis:"time_stamp"` // 时间戳
}

type Storer interface {
	Save(msg *sarama.ConsumerMessage) error
	Offset(topic string, partition int32) int64
	Init() error
}
