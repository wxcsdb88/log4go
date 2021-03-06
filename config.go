package log4go

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/kdpujie/log4go/util"
)

type ConfFileWriter struct {
	Level   string `json:"level"`
	LogPath string `json:"log_path"`
	On      bool   `json:"on"`
}

type ConfConsoleWriter struct {
	Level string `json:"level"`
	On    bool   `json:"on"`
	Color bool   `json:"color"`
}

type ConfAliLogHubWriter struct {
	Level           string `json:"level"`
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

// LogConfig log config
type LogConfig struct {
	Level           string              `json:"level"`
	FullPath        bool                `json:"fullPath"`
	FileWriter      ConfFileWriter      `json:"file_writer"`
	ConsoleWriter   ConfConsoleWriter   `json:"console_writer"`
	AliLoghubWriter ConfAliLogHubWriter `json:"ali_loghub_writer"`
	KafKaWriter     ConfKafKaWriter     `json:"kafka_writer"`
}

// SetupLog setup log
func SetupLog(lc LogConfig) (err error) {
	// 全局配置
	defaultLevel := getLevel(lc.Level)
	fullPath := lc.FullPath
	ShowFullPath(fullPath)

	if lc.FileWriter.On {
		w := NewFileWriter()
		w.level = getLevel0(lc.FileWriter.Level, defaultLevel)
		w.SetPathPattern(lc.FileWriter.LogPath)
		Register(w)
	}

	if lc.ConsoleWriter.On {
		w := NewConsoleWriter()
		w.level = getLevel0(lc.ConsoleWriter.Level, defaultLevel)
		w.SetColor(lc.ConsoleWriter.Color)
		Register(w)
	}

	if lc.AliLoghubWriter.On {
		w := NewAliLogHubWriter(lc.AliLoghubWriter.BufSize)
		if lc.AliLoghubWriter.LogSource == "" {
			lc.AliLoghubWriter.LogSource = util.GetLocalIpByTcp()
		}
		w.level = getLevel0(lc.AliLoghubWriter.Level, defaultLevel)
		w.SetLog(lc.AliLoghubWriter.LogName, lc.AliLoghubWriter.LogSource)
		w.SetProject(lc.AliLoghubWriter.ProjectName, lc.AliLoghubWriter.StoreName)
		w.SetEndpoint(lc.AliLoghubWriter.Endpoint)
		w.SetAccessKey(lc.AliLoghubWriter.AccessKeyId, lc.AliLoghubWriter.AccessKeySecret)
		Register(w)
	}

	if lc.KafKaWriter.On {
		w := NewKafKaWriter(&lc.KafKaWriter)
		w.level = getLevel0(lc.KafKaWriter.Level, defaultLevel)
		Register(w)
	}
	// 全局配置
	return nil
}

// SetupLogWithConf setup log with config file
func SetupLogWithConf(file string) (err error) {
	var lc LogConfig
	cnt, err := ioutil.ReadFile(file)

	if err = json.Unmarshal(cnt, &lc); err != nil {
		return
	}
	return SetupLog(lc)
}

func getLevel(flag string) int {
	return getLevel0(flag, DEBUG)
}

// 默认为Debug模式
func getLevel0(flag string, defaultFlag int) int {
	for i, f := range LEVEL_FLAGS {
		if strings.TrimSpace(strings.ToUpper(flag)) == f {
			return i
		}
	}
	fmt.Printf("[ERROR] 未找到合适的日志级别[%s]，使用默认值:%d \n", flag, defaultFlag)
	return defaultFlag
}
