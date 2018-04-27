package config

import (
	"github.com/spf13/viper"
	"fmt"
)

const PROFILE_KEY = "_profile_"

func LoadConsulConfigWithToken(host string, app string, profile string) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigName(app)
	v.Set(PROFILE_KEY, profile)
	v.AddRemoteProvider("consul", host, fmt.Sprintf("/config/%s::%s/", app, profile))
	v.SetConfigType("json") // because there is no file extension in a stream of bytes, supported extensions are "json", "toml", "yaml", "yml", "properties", "props", "prop"
	err := v.ReadRemoteConfig()
	return v, err
}
