package loghub

type LoghubConfig struct {
	Endpoint string
	AccessID string
	AccessSecret string
	Project string
	LogStore string

	RecorderAddr string
	RecorderUser string
	RecorderPass string
	RecorderDB string
}
