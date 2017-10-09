package conf

import (
	"github.com/spf13/viper"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

var Args Arguments

type Arguments struct {
	LogLevel string `mapstructure:"log_level"`
	Shard    int    `mapstructure:"shard"`
	Couchbase struct {
		Servers        string `mapstructure:"servers"`
		Bucket         string `mapstructure:"bucket"`
		Timeout        int    `mapstructure:"timeout"`
		MutationOpSize int    `mapstructure:"mutation_op_size"`
	}
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
	Args.Couchbase.Timeout = 5
	Args.Couchbase.MutationOpSize = 16
	Args.Kdj.CleanInterval = 2
}
