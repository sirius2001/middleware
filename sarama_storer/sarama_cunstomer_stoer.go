package sarama_storer

import (
	"sync/atomic"

	"github.com/IBM/sarama"
)

type DefalutConsumeInterceptor struct {
	storer    Storer
	messages  chan *sarama.ConsumerMessage
	maxoffset atomic.Int64
}

// OnConsume implements sarama.ConsumerInterceptor.
func (d *DefalutConsumeInterceptor) OnConsume(message *sarama.ConsumerMessage) {
	if d.maxoffset.Load() > message.Offset {
		return
	}

	if len(d.messages) < cap(d.messages) {
		d.messages <- message
	}

	if err := d.storer.Save(message); err == nil {
		d.maxoffset.Store(message.Offset)
	}
}

func NewDefalutConsumeInterceptor(storer Storer) sarama.ConsumerInterceptor {
	if storer == nil {
		panic("storer is nil")
	}

	if err := storer.Init(); err != nil {
		panic(err)
	}

	d := &DefalutConsumeInterceptor{
		storer:   storer,
		messages: make(chan *sarama.ConsumerMessage, 1024),
	}

	go d.recvLoop()

	return d
}

func (d *DefalutConsumeInterceptor) recvLoop() {
	for m := range d.messages {
		d.storer.Save(m)
	}
}
