package kafka

type KafkaConfig struct {
	kafkaHosts string
	DMLTopic string

	RecorderAddr string
	RecorderDB string
}
