package log4go

import (
	"encoding/json"
	"errors"
	"io/ioutil"
)

type ConfFileWriter struct {
	logPath string `json:"log_path"`
	on      bool   `json:"on"`
}

type ConfConsoleWriter struct {
	on    bool `json:"on"`
	color bool `json:"color"`
}

type ConfAliLogHubWriter struct {
	on              bool   `json:"on"`
	logName         string `json:"log_name"`
	logSource       string `json:"log_source"`
	projectName     string `json:"project_name"`
	endpoint        string `json:"endpoint"`
	accessKeyId     string `json:"access_key_id"`
	accessKeySecret string `json:"access_key_secret"`
	storeName       string `json:"store_name"`
	bufSize         string `json:"buf_size"`
}

type LogConfig struct {
	level           string              `json:"level"`
	fileWriter      ConfFileWriter      `json:"file_writer"`
	consoleWriter   ConfConsoleWriter   `json:"console_writer"`
	aliLoghubWriter ConfAliLogHubWriter `json:"ali_loghub_writer"`
}

func SetupLogWithConf(file string) (err error) {
	var lc LogConfig

	cnt, err := ioutil.ReadFile(file)

	if err = json.Unmarshal(cnt, &lc); err != nil {
		return
	}

	if lc.fileWriter.on {
		w := NewFileWriter()
		w.SetPathPattern(lc.fileWriter.logPath)
		Register(w)
	}

	if lc.consoleWriter.on {
		w := NewConsoleWriter()
		w.SetColor(lc.consoleWriter.color)
		Register(w)
	}

	if lc.aliLoghubWriter.on {

	}

	switch lc.level {
	case "debug":
		SetLevel(DEBUG)

	case "info":
		SetLevel(INFO)

	case "warning":
		SetLevel(WARNING)

	case "error":
		SetLevel(ERROR)

	case "fatal":
		SetLevel(FATAL)

	default:
		err = errors.New("Invalid log level")
	}
	return
}
