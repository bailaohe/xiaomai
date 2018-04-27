package config

import (
	"bytes"
	"github.com/spf13/viper"
	crypt "github.com/xordataexchange/crypt/config"
	"os"
	"io"
	"encoding/json"
	"strings"
)

type remoteConfigProvider struct{}

func makeConfDict(kvPairs crypt.KVPairs, prefix string) (*map[string]interface{}) {
	prefixLen := len(prefix)
	if prefix[0] == '/' {
		prefixLen --
	}
	if prefix[len(prefix)-1] != '/' {
		prefixLen ++
	}
	kvDict := make(map[string]interface{})

	for _, kv := range kvPairs {
		node := kvDict
		toks := strings.Split(kv.Key[prefixLen:], "/")
		for idx, tok := range toks {
			if idx < len(toks)-1 {
				if _, ok := node[tok]; !ok {
					node[tok] = make(map[string]interface{})
				}
				node = node[tok].(map[string]interface{})
			} else {
				node[tok] = string(kv.Value)
			}
		}
	}
	return &kvDict
}

func (rc remoteConfigProvider) Get(rp viper.RemoteProvider) (io.Reader, error) {
	cm, err := getConfigManager(rp)
	if err != nil {
		return nil, err
	}
	kvList, err := cm.List(rp.Path())

	if err != nil {
		return nil, err
	}

	byteConf, _ := json.Marshal(makeConfDict(kvList, rp.Path()))

	return bytes.NewReader(byteConf), nil
}

func (rc remoteConfigProvider) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	cm, err := getConfigManager(rp)
	if err != nil {
		return nil, err
	}
	resp, err := cm.Get(rp.Path())
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(resp), nil
}

func (rc remoteConfigProvider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	cm, err := getConfigManager(rp)
	if err != nil {
		return nil, nil
	}
	quit := make(chan bool)
	quitwc := make(chan bool)
	viperResponsCh := make(chan *viper.RemoteResponse)
	cryptoResponseCh := cm.Watch(rp.Path(), quit)
	// need this function to convert the Channel response form crypt.Response to viper.Response
	go func(cr <-chan *crypt.Response, vr chan<- *viper.RemoteResponse, quitwc <-chan bool, quit chan<- bool) {
		for {
			select {
			case <-quitwc:
				quit <- true
				return
			case resp := <-cr:
				vr <- &viper.RemoteResponse{
					Error: resp.Error,
					Value: resp.Value,
				}

			}

		}
	}(cryptoResponseCh, viperResponsCh, quitwc, quit)

	return viperResponsCh, quitwc
}

func getConfigManager(rp viper.RemoteProvider) (crypt.ConfigManager, error) {
	var cm crypt.ConfigManager
	var err error

	if rp.SecretKeyring() != "" {
		kr, err := os.Open(rp.SecretKeyring())
		defer kr.Close()
		if err != nil {
			return nil, err
		}
		if rp.Provider() == "etcd" {
			cm, err = crypt.NewEtcdConfigManager([]string{rp.Endpoint()}, kr)
		} else {
			cm, err = crypt.NewConsulConfigManager([]string{rp.Endpoint()}, kr)
		}
	} else {
		if rp.Provider() == "etcd" {
			cm, err = crypt.NewStandardEtcdConfigManager([]string{rp.Endpoint()})
		} else {
			cm, err = crypt.NewStandardConsulConfigManager([]string{rp.Endpoint()})
		}
	}
	if err != nil {
		return nil, err
	}
	return cm, nil
}

func init() {
	viper.RemoteConfig = &remoteConfigProvider{}
}
