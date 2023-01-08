package utils

import (
	"github.com/spf13/viper"
)

type Config struct {
	TenantHostname string `mapstructure:"TENANT_K8S_NODE"`
}

func LoadConfig(fileName string) (*Config, error) {
	configReader := viper.New()
	// set the config file type
	configReader.SetConfigType("env")

	// specify where the config file is
	//configReader.SetConfigFile()

	configReader.AddConfigPath("/env")
	configReader.SetConfigName(fileName)
	err := configReader.ReadInConfig()
	if err != nil {
		return nil, err
	}
	config := Config{}
	err = configReader.Unmarshal(&config)
	return &config, err
}
