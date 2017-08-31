package conf

import (
	"github.com/spf13/viper"
	"github.com/sirupsen/logrus"
)

var Args ArgsStruct

type ArgsStruct struct {
	CouchbaseServers string
}

func init() {
	viper.SetConfigName("rima") // name of config file (without extension)
	viper.AddConfigPath(".")      // optionally look for config in the working directory
	viper.AddConfigPath("$GOPATH/bin")
	viper.AddConfigPath("$HOME")
	err := viper.Unmarshal(&Args) // Find and read the config file
	if err != nil {
		logrus.Printf("config file error: %s", err)
	} else {
		viper.WatchConfig()
	}
}
