package sarama_storer

import (
	"fmt"

	"github.com/IBM/sarama"
	"gorm.io/gorm"
)

type DBStorer struct {
	db *gorm.DB
}

func NewDBStorer(db *gorm.DB) Storer {
	return &DBStorer{
		db: db,
	}
}

// Read implements sarama_storer.Storer.
func (d *DBStorer) Offset(topic string, partition int32) int64 {
	p := new(MessageRecord)
	if err := d.db.Where("id = ?", fmt.Sprintf("%s-%d", topic, partition)).First(p).Error; err != nil {
		return 0
	}
	return p.Offset + 1
}

// Init implements sarama_storer.Storer.
func (d *DBStorer) Init() error {
	p := new(MessageRecord)
	return d.db.AutoMigrate(p)
}

// Save implements sarama_storer.Storer.
func (d *DBStorer) Save(message *sarama.ConsumerMessage) error {
	r := &MessageRecord{
		ID:        fmt.Sprintf("%s-%d", message.Topic, message.Partition),
		Offset:    message.Offset,
		Partition: message.Partition,
		Topic:     message.Topic,
		TimeStamp: message.Timestamp,
	}
	return d.db.Save(r).Error
}
