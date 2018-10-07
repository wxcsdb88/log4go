package log4go

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// ConfKafKaWriter kafka writer conf
type ConfKafKaWriter struct {
	Level      int  `json:"level"`
	On         bool `json:"on"`
	BufferSize int  `json:"BufferSize"`
	Debug      bool `json:"debug"` // if true, will output the send msg

	Key string `json:"key"` // kafka producer key, temp set, choice field

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

	active bool // default true, if connect failed or timeout will false
	stop   chan bool
}

// NewKafKaWriter new kafka writer
func NewKafKaWriter(conf *ConfKafKaWriter) *KafKaWriter {
	stop := make(chan bool, 1)
	return &KafKaWriter{
		conf: conf,
		stop: stop,
	}
}

// Init service for Record
func (k *KafKaWriter) Init() error {
	err := k.Start()
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

	key := ""
	if k.conf.Key != "" {
		key = k.conf.Key
	}

	msg := &sarama.ProducerMessage{
		Topic:     k.conf.ProducerTopic,
		Timestamp: time.Now().Local(),
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(data),
	}

	if k.conf.Debug {
		fmt.Printf("kafka-writer msg [topic: %v, partition: %v, offset %v, timestamp: %v]\nkey:   %v\nvalue: %v\n", msg.Topic,
			msg.Partition, msg.Offset, msg.Timestamp, key, data)
	}
	go k.asyncWriteMessages(msg)

	return nil
}

func (k *KafKaWriter) asyncWriteMessages(msg *sarama.ProducerMessage) {
	if msg != nil {
		k.messages <- msg
	}
}

// send kafka message to kafka
func (k *KafKaWriter) daemonProducer() {
	for {
		mes, ok := <-k.messages
		if !ok {
			k.stop <- true
			return
		}
		// mes.Topic = k.conf.ProducerTopic
		_, _, err := k.producer.SendMessage(mes)
		if err != nil {
			Error("SendMessage(topic=%s, key=%v, value=%v) err=%s \n", mes.Topic, mes.Key, mes.Value, err.Error())
		}
	}
}

// Start start the kafka writer
func (k *KafKaWriter) Start() (err error) {
	fmt.Print("start kafka writer ....\n")
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = k.conf.ProducerReturnSuccesses
	cfg.Producer.Timeout = time.Duration(k.conf.ProducerTimeout) * time.Millisecond
	k.producer, err = sarama.NewSyncProducer(k.conf.Brokers, cfg)
	if err != nil {
		fmt.Printf("sarama.NewSyncProducer err, message=%s \n", err)
		k.stop <- true
		return err
	}
	size := k.conf.BufferSize
	if size <= 1 {
		size = 1
	}
	k.messages = make(chan *sarama.ProducerMessage, size)

	go k.daemonProducer()
	fmt.Print("start kafka writer ok\n")
	return err
}

// Stop stop the kafka writer
func (k *KafKaWriter) Stop() {
	close(k.messages)
	<-k.stop
	k.producer.Close()
}
