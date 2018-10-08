package main

import (
	log "github.com/kdpujie/log4go"
)

// SetLog set logger
func SetLog() {
	w1 := log.NewFileWriterWithLevel(log.ERROR)
	w1.SetPathPattern("/tmp/logs/error%Y%M%D%H.log")

	w2 := log.NewConsoleWriterWithLevel(log.WARNING)

	log.Register(w1)
	log.Register(w2)
}

func main() {
	SetLog()
	defer log.Close()

	var name = "skoo"
	log.Debug("log4go by %s", name)
	log.Info("log4go by %s", name)
	log.Warn("log4go by %s", name)
	log.Error("log4go by %s", name)
	log.Fatal("log4go by %s", name)
}
