package sarama_storer

import (
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

type SyncProducerStorer struct {
	storer  Storer
	records chan *ProducerRecord
	sarama.SyncProducer
}

// conf.Consumer.Interceptors = append(conf.Consumer.Interceptors, )
func NewSyncProducerStore(storer Storer, producer sarama.SyncProducer) (*SyncProducerStorer, error) {
	if storer == nil {
		return nil, errors.New("db is nil,system error")
	}

	if producer == nil {
		return nil, errors.New("producer is nil,system error")
	}

	if err := storer.Init(); err != nil {
		return nil, err
	}

	p := &SyncProducerStorer{
		storer:       storer,
		records:      make(chan *ProducerRecord, 1024),
		SyncProducer: producer,
	}

	go p.revLoop()

	return p, nil
}

func (d *SyncProducerStorer) Offset(topic string, partition int32) (int64, error) {
	p, err := d.storer.Read(topic, partition)
	if err != nil || p == nil {
		return 0, err
	}
	return p.Offset, err
}

func (d *SyncProducerStorer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	partition, offset, err := d.SyncProducer.SendMessage(msg)
	if err == nil {
		p := &ProducerRecord{
			ID:        fmt.Sprintf("%s-%d", msg.Topic, msg.Partition),
			Offset:    offset,
			Partition: partition,
			Topic:     msg.Topic,
			TimeStamp: msg.Timestamp,
		}

		select {
		case d.records <- p:

		default:
			if err := d.storer.Save(p); err != nil {
			}
		}
	}

	return partition, offset, err
}

func (d *SyncProducerStorer) SendMessages(msgs []*sarama.ProducerMessage) error {
	if err := d.SyncProducer.SendMessages(msgs); err != nil {
		return err
	}
	msg := msgs[len(msgs)-1]

	p := &ProducerRecord{
		ID:        fmt.Sprintf("%s-%d", msg.Topic, msg.Partition),
		Offset:    msg.Offset,
		Partition: msg.Partition,
		Topic:     msg.Topic,
		TimeStamp: msg.Timestamp,
	}
	select {
	case d.records <- p:

	default:
		if err := d.storer.Save(p); err != nil {
		}
	}

	return nil
}

func (d *SyncProducerStorer) revLoop() {
	for r := range d.records {
		if err := d.storer.Save(r); err != nil {
			continue
		}
	}
}
