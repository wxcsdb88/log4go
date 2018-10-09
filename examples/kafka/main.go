package main

import (
	"time"

	log "github.com/kdpujie/log4go"
)

// SetLog set logger
func SetLog() {
	w1 := log.NewConsoleWriterWithLevel(log.DEBUG)

	kafKaConf := &log.ConfKafKaWriter{
		Debug:                   true,
		Key:                     "test",
		BufferSize:              1,
		On:                      true,
		ProducerTopic:           "kafka1",
		ProducerReturnSuccesses: true,
		ProducerTimeout:         100,
		Brokers:                 []string{"127.0.0.1:9092"},
		// Brokers: []string{"localhost:9092"},
		MSG: log.KafKaMSGFields{
			ExtraFields: map[string]interface{}{
				"appId":    188,
				"appEnv":   "test",
				"hostname": "futurever",
				"keys":     123,
				"chain":    "3123",
			},
		},
	}
	w2 := log.NewKafKaWriterWithWriter(kafKaConf, log.ERROR)

	w1.SetColor(true)

	log.Register(w1)
	log.Register(w2)

	kafKaConf2 := *kafKaConf
	kafKaConf2.Level = "WARN"
	kafKaConf2.ProducerTopic = "kafka2"
	w3 := log.NewKafKaWriter(&kafKaConf2)
	log.Register(w3)
}

func main() {
	SetLog()
	defer log.Close()

	var name = "kafka-writer"

	for i := 0; i < 1; i++ {
		log.Debug("log4go by %s debug", name)
		log.Info("log4go by %s info", name)
		log.Warn("log4go by %s warn", name)
		log.Error("log4go by %s error", name)
		log.Fatal("log4go by %s fatal", name)
	}
	time.Sleep(1 * time.Second)
}
