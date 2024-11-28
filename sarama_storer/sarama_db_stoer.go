package sarama_storer

import (
	"fmt"

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
func (d *DBStorer) Read(topic string, partition int32) (*ProducerRecord, error) {
	p := new(ProducerRecord)
	return p, d.db.Where("id = ?", fmt.Sprintf("%s-%d", topic, partition)).First(p).Error
}

// Init implements sarama_storer.Storer.
func (d *DBStorer) Init() error {
	p := new(ProducerRecord)
	return d.db.AutoMigrate(p)
}

// Save implements sarama_storer.Storer.
func (d *DBStorer) Save(data *ProducerRecord) error {
	return d.db.Save(data).Error
}
