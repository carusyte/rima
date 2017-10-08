package conf

import (
	"github.com/spf13/viper"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

var Args Arguments

type Arguments struct {
	CouchbaseServers string `mapstructure:"couchbase_servers"`
	LogLevel         string `mapstructure:"log_level"`
	Shard            int    `mapstructure:"shard"`
	CouchbaseTimeout int    `mapstructure:"couchbase_timeout"`
	Kdj struct {
		CleanInterval time.Duration `mapstructure:"clean_interval"`
	}
}

func init() {
	setDefaults()
	v := viper.New()
	v.SetConfigName("rima") // name of config file (without extension)
	v.AddConfigPath(".")    // optionally look for config in the working directory
	v.AddConfigPath("$GOPATH/bin")
	v.AddConfigPath("$HOME")
	v.AddConfigPath("$HOME/go/bin")
	err := v.ReadInConfig()
	if err != nil {
		logrus.Errorf("config file error: %+v", err)
		return
	}
	err = v.Unmarshal(&Args) // Find and read the config file
	if err != nil {
		logrus.Errorf("config file error: %s", err)
	} else {
		//v.WatchConfig()
		var level logrus.Level
		switch Args.LogLevel {
		case "debug":
			level = logrus.DebugLevel
		case "info":
			level = logrus.InfoLevel
		case "warning":
			level = logrus.WarnLevel
		case "error":
			level = logrus.ErrorLevel
		case "fatal":
			level = logrus.FatalLevel
		case "panic":
			level = logrus.PanicLevel
		}
		logrus.SetLevel(level)
		logrus.SetOutput(os.Stderr)
		logrus.Debugf("Configuration: %+v", Args)
	}
}

func setDefaults() {
	Args.LogLevel = "info"
	Args.Shard = 20
	Args.CouchbaseTimeout = 5
	Args.Kdj.CleanInterval = 2
}
