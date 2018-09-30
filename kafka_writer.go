package log4go

import (
	"time"

	"github.com/Shopify/sarama"
)

// ConfKafKaWriter kafka writer conf
type ConfKafKaWriter struct {
	Level int  `json:"level"`
	On    bool `json:"on"`

	ProducerTopic           string   `json:"producerTopic"`
	ProducerReturnSuccesses bool     `json:"producerReturnSuccesses"`
	ProducerTimeout         int64    `json:"producerTimeout"` //ms
	Brokers                 []string `json:"brokers"`
}

// KafKaWriter kafka writer
type KafKaWriter struct {
	producer sarama.SyncProducer
	messages chan *sarama.ProducerMessage
	conf     *ConfKafKaWriter

	f chan bool
}

// NewKafKaWriter new kafka writer
func NewKafKaWriter(conf *ConfKafKaWriter) *KafKaWriter {
	return &KafKaWriter{
		conf: conf,
	}
}

// Init service for Record
func (k *KafKaWriter) Init() error {
	err := k.Start()
	Info("start kafka writer")
	if err != nil {
		Error("Init err=%s \n", err.Error())
	}
	return nil
}

// Write service for Record
func (k *KafKaWriter) Write(r *Record) error {
	if r.level < k.conf.Level {
		return nil
	}

	data := r.info
	if data == "" {
		return nil
	}
	k.messages <- &sarama.ProducerMessage{
		Key:   sarama.ByteEncoder(k.conf.ProducerTopic),
		Value: sarama.ByteEncoder(data),
	}
	return nil
}

// send kafka message to kafka
func (k *KafKaWriter) daemonProducer() {
	for {
		mes, ok := <-k.messages
		if !ok {
			k.f <- true
			return
		}
		mes.Topic = k.conf.ProducerTopic
		_, _, err := k.producer.SendMessage(mes)
		if err != nil {
			Error("SendMessage(%s) err=%s \n", mes.Value, err.Error())
		}
	}
}

// Start start the kafka writer
func (k *KafKaWriter) Start() (err error) {
	Info("start kafka writer ....")
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = k.conf.ProducerReturnSuccesses
	cfg.Producer.Timeout = time.Duration(k.conf.ProducerTimeout) * time.Millisecond
	k.producer, err = sarama.NewSyncProducer(k.conf.Brokers, cfg)
	if err != nil {
		Fatal("sarama.NewSyncProducer err, message=%s \n", err)
		return err
	}

	go k.daemonProducer()
	Info("start kafka writer ok")
	return err
}

// Stop stop the kafka writer
func (k *KafKaWriter) Stop() {
	close(k.messages)
	<-k.f
	k.producer.Close()
}
