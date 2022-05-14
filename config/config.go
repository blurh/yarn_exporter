package config

import (
    "flag"

    "github.com/spf13/viper"
)

type Config struct {
    YarnEndpointScheme             string
    YarnEndpointHost               string
    YarnEndpointClusterMetricsPort string
    YarnEndpointClusterMetricsPath string
    YarnEndpointApplicationPort    string
    YarnEndpointApplicationPath    string
    Listen                         string
}

func (c *Config) LoadConfig() {
    var configFile string
    flag.StringVar(&configFile, "config", "./config.yaml", "config file path")
    flag.Parse()

    vConfig := viper.New()
    vConfig.SetConfigFile(configFile)
    vConfig.ReadInConfig()

    c.YarnEndpointScheme = vConfig.GetString("exporter.yarn.endpoint.scheme")
    c.YarnEndpointHost = vConfig.GetString("exporter.yarn.endpoint.host")
    c.YarnEndpointClusterMetricsPort = vConfig.GetString("exporter.yarn.endpoint.cluster_metrics.port")
    c.YarnEndpointClusterMetricsPath = vConfig.GetString("exporter.yarn.endpoint.cluster_metrics.path")
    c.YarnEndpointApplicationPort = vConfig.GetString("exporter.yarn.endpoint.application.port")
    c.YarnEndpointApplicationPath = vConfig.GetString("exporter.yarn.endpoint.application.path")

    c.Listen = vConfig.GetString("exporter.listen")
}

func NewConfig() *Config {
    c := &Config{}
    c.LoadConfig()
    return c
}
