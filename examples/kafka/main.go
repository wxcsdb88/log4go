package main

import (
	"time"

	log "github.com/kdpujie/log4go"
)

// SetLog set logger
func SetLog() {
	w1 := log.NewConsoleWriter()

	kafKaConf := &log.ConfKafKaWriter{
		Debug:                   true,
		Key:                     "test",
		BufferSize:              1,
		Level:                   log.ERROR,
		On:                      true,
		ProducerTopic:           "kafka1",
		ProducerReturnSuccesses: true,
		ProducerTimeout:         100,
		Brokers:                 []string{"192.168.1.100:9092"},
	}
	w2 := log.NewKafKaWriter(kafKaConf)
	w1.Level = log.DEBUG
	w1.SetColor(true)

	log.Register(w1)
	log.Register(w2)

	kafKaConf2 := *kafKaConf
	kafKaConf2.Level = log.INFO
	kafKaConf2.ProducerTopic = "kafka2"
	w3 := log.NewKafKaWriter(&kafKaConf2)
	log.Register(w3)
}

func main() {
	SetLog()
	defer log.Close()

	var name = "kafka-writer"
	log.Debug("log4go by %s", name)
	log.Info("log4go by %s", name)
	log.Warn("log4go by %s", name)
	log.Error("log4go by %s", name)
	log.Fatal("log4go by %s", name)

	for i := 0; i <= 3; i++ {
		log.Debug("log4go by %s", name)
		log.Info("log4go by %s", name)
		log.Warn("log4go by %s", name)
		log.Error("log4go by %s", name)
		log.Fatal("log4go by %s", name)
	}
	time.Sleep(2 * time.Second)
}
