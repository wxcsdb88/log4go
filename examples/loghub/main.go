package main

import log "github.com/kdpujie/log4go"

var (
	ProjectName     = "happyelements-data"
	EndPoint        = "cn-beijing.log.aliyuncs.com"
	AccessKeyID     = "LTAIr66yTpB5LiK0"
	AccessKeySecret = "y1O3dgtHe3d2575jDGSKwwHAS9695c"
	LogStoreName    = "dsp_syslog"
)

func main() {
	w := log.NewAliLogHubWriter(2048)
	w.SetLog("log4go-test", "10.130.149.56")
	w.SetProject(ProjectName, LogStoreName)
	w.SetEndpoint(EndPoint)
	w.SetAccessKey(AccessKeyID, AccessKeySecret)
	log.Register(w)
	log.SetLevel(log.DEBUG)
	defer log.Close()

	log.Info("ali-log-hub Writer for log4go")
	log.Debug("ali-log-hub Writer for log4go")
	log.Error("ali-log-hub Writer for log4go")
}
