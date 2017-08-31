package conf

import (
	"github.com/spf13/viper"
	"github.com/sirupsen/logrus"
)

var Args Arguments

type Arguments struct {
	CouchbaseServers string `mapstructure:"couchbase_servers"`
}

func init() {
	v := viper.New()
	v.SetConfigName("rima") // name of config file (without extension)
	v.AddConfigPath(".")      // optionally look for config in the working directory
	v.AddConfigPath("$GOPATH/bin")
	v.AddConfigPath("$HOME")
	err := v.ReadInConfig()
	if err != nil {
		logrus.Errorf("config file error: %+v", err)
		return
	}
	err = v.Unmarshal(&Args) // Find and read the config file
	if err != nil {
		logrus.Printf("config file error: %s", err)
	} else {
		logrus.Printf("Configuration: %+v", Args)
		v.WatchConfig()
	}
}
