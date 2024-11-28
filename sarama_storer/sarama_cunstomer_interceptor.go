package sarama_storer

import (
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type DefalutConsumeInterceptor struct {
	storer   Storer
	messages chan *sarama.ConsumerMessage
	data     map[string]*sarama.ConsumerMessage
	dataLock sync.Mutex
}

// OnConsume implements sarama.ConsumerInterceptor.
func (d *DefalutConsumeInterceptor) OnConsume(message *sarama.ConsumerMessage) {
	d.dataLock.Lock()
	defer d.dataLock.Unlock()

	key := fmt.Sprintf("%s-%d", message.Topic, message.Partition)
	if m, exits := d.data[key]; exits {
		if message.Offset > m.Offset {
			d.data[key] = message
		}
	} else {
		d.data[key] = message
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
		data:     make(map[string]*sarama.ConsumerMessage),
	}

	go func() {
		tracker := time.NewTicker(time.Second)
		defer tracker.Stop()
		for {
			<-tracker.C
			d.dataLock.Lock()
			for _, v := range d.data {
				d.storer.Save(v)
			}
			d.dataLock.Unlock()
		}
	}()

	return d
}
