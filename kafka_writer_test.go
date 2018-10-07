package log4go

import (
	"testing"
)

func getDefaultKafKaWriterConf() *ConfKafKaWriter {
	return &ConfKafKaWriter{
		Level:                   1,
		On:                      true,
		ProducerTopic:           "test",
		ProducerReturnSuccesses: true,
		ProducerTimeout:         30,
		Brokers:                 []string{""},
	}
}

func Test_NewConfKafKaWriter(t *testing.T) {
	conf := getDefaultKafKaWriterConf()
	kafKaWriter := NewKafKaWriter(conf)
	t.Log(kafKaWriter)

	err := kafKaWriter.Start()
	if err != nil {
		t.Error(err)
	}
}
