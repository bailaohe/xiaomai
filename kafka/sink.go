package kafka

import (
	"github.com/bailaohe/xiaomai/binlog"
	"github.com/juju/errors"
	"github.com/bwmarrin/snowflake"
	"time"
	"gopkg.in/mgo.v2"
	"github.com/siddontang/go-mysql/canal"
	"encoding/json"
	"github.com/Shopify/sarama"
	"strings"
)

type KafkaSink struct {
	producer sarama.SyncProducer
	idGen *snowflake.Node
	recorder *mgo.Session
	recordDB string
}

func (self *KafkaSink) Parse(e *canal.RowsEvent) ([]interface{}, error) {
	now := time.Now()

	payload := binlog.ParsePayload(e)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	eventRecord := binlog.NewEventRecord(now, string(payloadBytes))
	err = self.recorder.DB(self.recordDB).C(binlog.RECORDER_COLLECTION).Insert(eventRecord)
	if err != nil {
		return nil, err
	}

	message := map[string]string{
		"id": eventRecord.ID.Hex(),
		"level": "EVENT",
		"timestamp": now.Format(binlog.DATE_FORMAT),
		"eventClass": "com.xiaomai.canal.event.DMLChangeEvent",
		"payload": string(payloadBytes),
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	logs := []interface{}{
		&sarama.ProducerMessage{
			Topic: "DMLChangeEvent",
			Value: sarama.ByteEncoder(messageBytes),
		},
	}
	return logs, nil
}

func (self *KafkaSink) Publish(reqs []interface{}) error {

	var logs []*sarama.ProducerMessage
	for _, req := range reqs {
		logs = append(logs, req.(*sarama.ProducerMessage))
	}

	err := self.producer.SendMessages(logs)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func NewKafkaSink(conf *KafkaConfig) (*KafkaSink, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	p, err := sarama.NewSyncProducer(strings.Split(conf.KafkaHosts, ","), config)
	if err != nil {
		return nil, err
	}

	node, err := snowflake.NewNode(binlog.MYSQL_SYNC_SERVICE_ID)
	if err != nil {
		return nil, err
	}

	session, err := mgo.Dial(conf.RecorderAddr)
	if err != nil {
		return nil, err
	}

	return &KafkaSink{
		producer: &p,
		idGen: node,
		recorder: session,
		recordDB: conf.RecorderDB,
	}, nil
}
