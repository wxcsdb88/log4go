package log4go

import (
	"fmt"
	"os"
)

type colorRecord Record

func (r *colorRecord) String() string {
	switch r.level {
	case DEBUG:
		return fmt.Sprintf("\033[36m%s\033[0m [\033[34m%s\033[0m] \033[47;30m%s\033[0m %s\n",
			r.time, LEVEL_FLAGS[r.level], r.code, r.info)

	case INFO:
		return fmt.Sprintf("\033[36m%s\033[0m [\033[32m%s\033[0m] \033[47;30m%s\033[0m %s\n",
			r.time, LEVEL_FLAGS[r.level], r.code, r.info)

	case WARNING:
		return fmt.Sprintf("\033[36m%s\033[0m [\033[33m%s\033[0m] \033[47;30m%s\033[0m %s\n",
			r.time, LEVEL_FLAGS[r.level], r.code, r.info)

	case ERROR:
		return fmt.Sprintf("\033[36m%s\033[0m [\033[31m%s\033[0m] \033[47;30m%s\033[0m %s\n",
			r.time, LEVEL_FLAGS[r.level], r.code, r.info)

	case FATAL:
		return fmt.Sprintf("\033[36m%s\033[0m [\033[35m%s\033[0m] \033[47;30m%s\033[0m %s\n",
			r.time, LEVEL_FLAGS[r.level], r.code, r.info)
	}

	return ""
}

// ConsoleWriter console writer define
type ConsoleWriter struct {
	Level int
	color bool
}

// NewConsoleWriter create new console writer
func NewConsoleWriter() *ConsoleWriter {
	return &ConsoleWriter{}
}

// NewConsoleWriterWithLevel create new console writer with level
func NewConsoleWriterWithLevel(level int) *FileWriter {
	defaultLevel := DEBUG
	maxLevel := len(LEVEL_FLAGS)
	if maxLevel >= 1 {
		maxLevel = maxLevel - 1
	}

	if level >= defaultLevel && level <= maxLevel {
		defaultLevel = level
	}
	return &FileWriter{
		Level: defaultLevel,
	}
}

// Write console write
func (w *ConsoleWriter) Write(r *Record) error {
	if r.level < w.Level {
		return nil
	}
	if w.color {
		fmt.Fprint(os.Stdout, ((*colorRecord)(r)).String())
	} else {
		fmt.Fprint(os.Stdout, r.String())
	}
	return nil
}

// Init console init without implement
func (w *ConsoleWriter) Init() error {
	return nil
}

// SetColor console output color control
func (w *ConsoleWriter) SetColor(c bool) {
	w.color = c
}
