package utils

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"k8s.io/client-go/rest"
)

var TenantRestConfig *rest.Config

type Config struct {
	TenantHostname string `mapstructure:"TENANT_K8S_NODE"`
}

func loadConfig(fileName string) (*Config, error) {
	configReader := viper.New()
	// set the config file type
	configReader.SetConfigType("env")

	// specify where the config file is

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

// Currently mapping is provided by CM, it is not optimal and should be changed to automatic node labeling one day
func GetMatchedTenantNode(nodeName string, log logrus.FieldLogger) (string, error) {
	tenantConfig, err := loadConfig(nodeName)
	if err != nil {
		log.WithError(err).Errorf("Failed to get matched tenant node from configmap mount file for node %s", nodeName)
		return "", nil
	}
	log.Infof("Found tenant node %s", tenantConfig.TenantHostname)
	return tenantConfig.TenantHostname, nil
}
