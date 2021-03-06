package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/bailaohe/xiaomai/binlog"
	"github.com/bwmarrin/snowflake"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"gopkg.in/mgo.v2"
	"strconv"
)

type KafkaSink struct {
	producer *kafka.Writer
	idGen    *snowflake.Node
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

	var id string

	if self.recorder != nil {
		eventRecord := binlog.NewEventRecord(now, string(payloadBytes))
		err = self.recorder.DB(self.recordDB).C(binlog.RECORDER_COLLECTION).Insert(eventRecord)
		if err != nil {
			return nil, err
		}

		id = eventRecord.ID.Hex()
	} else {
		id = self.idGen.Generate().String()
	}

	headers := []kafka.Header{
		{"XMEventClass", []byte("com.xiaomai.event.DBSyncEvent")},
		{"XMEventTriggerTime", []byte(strconv.FormatInt(now.Unix(), 10))},
		{"XMEventId", []byte(id)},
	}

	logs := []interface{}{
		&kafka.Message{
			Key:   []byte(id),
			Value: payloadBytes,
			Headers: headers,
		},
	}
	return logs, nil
}

func (self *KafkaSink) Publish(reqs []interface{}) error {

	var logs []kafka.Message
	for _, req := range reqs {
		logs = append(logs, *req.(*kafka.Message))
	}

	err := self.producer.WriteMessages(context.Background(), logs...)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func NewKafkaSink(conf *KafkaConfig) (*KafkaSink, error) {
	p := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers: strings.Split(conf.KafkaHosts, ","),
			Topic:   "DBSyncEvent",
		})

	if conf.EnableRecorder {
		session, err := mgo.Dial(conf.RecorderAddr)
		if err != nil {
			return nil, err
		}
		return &KafkaSink{
			producer: p,
			idGen:    nil,
			recorder: session,
			recordDB: conf.RecorderDB,
		}, nil
	}

	node, err := snowflake.NewNode(binlog.MYSQL_SYNC_SERVICE_ID)
	if err != nil {
		return nil, err
	}
	return &KafkaSink{
		producer: p,
		idGen:    node,
		recorder: nil,
		recordDB: "",
	}, nil
}
