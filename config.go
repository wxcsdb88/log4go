package log4go

import (
	"encoding/json"
	"fmt"
	"github.com/kdpujie/log4go/util"
	"io/ioutil"
)

type ConfFileWriter struct {
	LogPath string `json:"log_path"`
	On      bool   `json:"on"`
}

type ConfConsoleWriter struct {
	On    bool `json:"on"`
	Color bool `json:"color"`
}

type ConfAliLogHubWriter struct {
	On              bool   `json:"on"`
	LogName         string `json:"log_name"`
	LogSource       string `json:"log_source"`
	ProjectName     string `json:"project_name"`
	Endpoint        string `json:"endpoint"`
	AccessKeyId     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	StoreName       string `json:"store_name"`
	BufSize         int    `json:"buf_size"`
}

type LogConfig struct {
	Level           string              `json:"level"`
	FileWriter      ConfFileWriter      `json:"file_writer"`
	ConsoleWriter   ConfConsoleWriter   `json:"console_writer"`
	AliLoghubWriter ConfAliLogHubWriter `json:"ali_loghub_writer"`
}

func SetupLogWithConf(file string) (err error) {
	var lc LogConfig

	cnt, err := ioutil.ReadFile(file)

	if err = json.Unmarshal(cnt, &lc); err != nil {
		return
	}

	if lc.FileWriter.On {
		w := NewFileWriter()
		w.SetPathPattern(lc.FileWriter.LogPath)
		Register(w)
	}

	if lc.ConsoleWriter.On {
		w := NewConsoleWriter()
		w.SetColor(lc.ConsoleWriter.Color)
		Register(w)
	}

	if lc.AliLoghubWriter.On {
		w := NewAliLogHubWriter(lc.AliLoghubWriter.BufSize)
		if lc.AliLoghubWriter.LogSource == "" {
			lc.AliLoghubWriter.LogSource = util.GetLocalIpByTcp()
		}
		w.SetLog(lc.AliLoghubWriter.LogName, lc.AliLoghubWriter.LogSource)
		w.SetProject(lc.AliLoghubWriter.ProjectName, lc.AliLoghubWriter.StoreName)
		w.SetEndpoint(lc.AliLoghubWriter.Endpoint)
		w.SetAccessKey(lc.AliLoghubWriter.AccessKeyId, lc.AliLoghubWriter.AccessKeySecret)
		Register(w)
	}

	switch lc.Level {
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
		err = fmt.Errorf("request level[debug|info|warning|error|fatal], not suported[%s]", lc.Level)
	}
	return
}
