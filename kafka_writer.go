package log4go

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

const timestampFormat = "2006-01-02T15:04:05.000+0800"

// KafKaMSGFields kafka msg fields
type KafKaMSGFields struct {
	ESIndex     string                 `json:"esIndex"` // required, init field
	level       string                 // dynamic, set by logger
	Message     string                 `json:"message"`     // required, dynamic
	ServerIP    string                 `json:"serverIp"`    // required, init field, set by app
	Timestamp   string                 `json:"timeStamp"`   // required, dynamic, set by logger
	Now         int64                  `json:"now"`         // choice
	ExtraFields map[string]interface{} `json:"extraFields"` // extra fields will be added
}

// ConfKafKaWriter kafka writer conf
type ConfKafKaWriter struct {
	Level      string `json:"level"`
	On         bool   `json:"on"`
	BufferSize int    `json:"bufferSize"`
	Debug      bool   `json:"debug"` // if true, will output the send msg

	Key string `json:"key"` // kafka producer key, temp set, choice field

	ProducerTopic           string   `json:"producerTopic"`
	ProducerReturnSuccesses bool     `json:"producerReturnSuccesses"`
	ProducerTimeout         int64    `json:"producerTimeout"` //ms
	Brokers                 []string `json:"brokers"`

	MSG KafKaMSGFields
}

// KafKaWriter kafka writer
type KafKaWriter struct {
	level    int
	producer sarama.SyncProducer
	messages chan *sarama.ProducerMessage
	conf     *ConfKafKaWriter

	stop chan bool
}

// NewKafKaWriter new kafka writer
func NewKafKaWriter(conf *ConfKafKaWriter) *KafKaWriter {
	stop := make(chan bool, 1)
	defaultLevel := 0
	if conf.Level != "" {
		defaultLevel = getLevel0(conf.Level, defaultLevel)
	}

	return &KafKaWriter{
		conf:  conf,
		stop:  stop,
		level: defaultLevel,
	}
}

// NewKafKaWriterWithWriter new kafka writer with level
func NewKafKaWriterWithWriter(conf *ConfKafKaWriter, level int) *KafKaWriter {
	stop := make(chan bool, 1)
	defaultLevel := DEBUG
	maxLevel := len(LEVEL_FLAGS)
	if maxLevel >= 1 {
		maxLevel = maxLevel - 1
	}

	if level >= defaultLevel && level <= maxLevel {
		defaultLevel = level
	}

	return &KafKaWriter{
		conf:  conf,
		stop:  stop,
		level: defaultLevel,
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
	if r.level < k.level {
		return nil
	}

	logMsg := r.info
	if logMsg == "" {
		return nil
	}
	data := k.conf.MSG
	// timestamp, level
	data.level = LEVEL_FLAGS[r.level]
	now := time.Now()
	data.Now = now.Unix()
	data.Timestamp = now.Format(timestampFormat)
	data.Message = logMsg

	byteData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	var structData map[string]interface{}
	json.Unmarshal(byteData, &structData)
	delete(structData, "extraFields")

	// not exist new fields will be added
	for k, v := range data.ExtraFields {
		if _, ok := structData[k]; !ok {
			structData[k] = v
		}
	}

	jsonStructDataByte, err := json.Marshal(structData)
	if err != nil {
		return err
	}

	jsonData := string(jsonStructDataByte)

	key := ""
	if k.conf.Key != "" {
		key = k.conf.Key
	}

	msg := &sarama.ProducerMessage{
		Topic: k.conf.ProducerTopic,
		// Timestamp: time.Now(), // auto generate
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(jsonData),
	}

	if k.conf.Debug {
		fmt.Printf("kafka-writer msg [topic: %v, timestamp: %v, brokers: %v]\nkey:   %v\nvalue: %v\n", msg.Topic,
			msg.Timestamp, k.conf.Brokers, key, jsonData)
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

		partition, offset, err := k.producer.SendMessage(mes)

		if err != nil {
			fmt.Printf("SendMessage(topic=%s, partition=%v, offset=%v, key=%s, value=%s,timstamp=%v) err=%s\n\n", mes.Topic,
				partition, offset, mes.Key, mes.Value, mes.Timestamp, err.Error())
			continue
		} else {
			if k.conf.Debug {
				fmt.Printf("SendMessage(topic=%s, partition=%v, offset=%v, key=%s, value=%s,timstamp=%v)\n\n", mes.Topic,
					partition, offset, mes.Key, mes.Value, mes.Timestamp)
			}
		}
	}
}

// Start start the kafka writer
func (k *KafKaWriter) Start() (err error) {
	fmt.Print("start kafka writer ....\n")
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = k.conf.ProducerReturnSuccesses
	cfg.Producer.Timeout = time.Duration(k.conf.ProducerTimeout) * time.Millisecond
	cfg.Version = sarama.MaxVersion // sarama.V2_0_0_0, if want set timestamp for data should set version

	// NewHashPartitioner returns a Partitioner which behaves as follows. If the message's key is nil then a
	// random partition is chosen. Otherwise the FNV-1a hash of the encoded bytes of the message key is used,
	// modulus the number of partitions. This ensures that messages with the same key always end up on the
	// same partition.
	// cfg.Producer.Partitioner = sarama.NewHashPartitioner
	// cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	cfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	// cfg.Producer.Partitioner = sarama.NewReferenceHashPartitioner

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
