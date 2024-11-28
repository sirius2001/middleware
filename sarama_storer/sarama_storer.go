package sarama_storer

type Storer interface {
	Save(data *ProducerRecord) error
	Read(topic string, partition int32) (*ProducerRecord, error)
	Init() error
}
