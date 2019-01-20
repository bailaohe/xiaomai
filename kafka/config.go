package kafka

type KafkaConfig struct {
	KafkaHosts string

	EnableRecorder bool
	RecorderAddr   string
	RecorderDB     string
}
