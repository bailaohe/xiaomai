package loghub

import (
	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/siddontang/go-mysql/canal"
	"time"
	"encoding/json"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/bwmarrin/snowflake"
	"gopkg.in/mgo.v2"
	"github.com/bailaohe/xiaomai/binlog"
)

type LoghubSink struct {
	logStore *sls.LogStore
	idGen    *snowflake.Node
	recorder *mgo.Session
	recordDB string
}

func (self *LoghubSink) Parse(e *canal.RowsEvent) ([]interface{}, error) {
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

	logs := []interface{}{
		&sls.Log{
			Time: proto.Uint32(uint32(now.Unix())),
			Contents: []*sls.LogContent{
				{Key: proto.String("id"), Value: proto.String(eventRecord.ID.Hex())},
				{Key: proto.String("level"), Value: proto.String("EVENT")},
				{Key: proto.String("@timestamp"), Value: proto.String(now.Format(binlog.DATE_FORMAT))},
				{Key: proto.String("eventClass"), Value: proto.String("com.xiaomai.canal.event.DMLChangeEvent")},
				{Key: proto.String("payload"), Value: proto.String(string(payloadBytes))},
			},
		},
	}
	return logs, nil
}

func (self *LoghubSink) Publish(reqs []interface{}) error {

	var logs []*sls.Log
	for _, req := range reqs {
		logs = append(logs, req.(*sls.Log))
	}

	logGroup := &sls.LogGroup{
		Logs:   logs,
		Source: proto.String(""),
		Topic:  proto.String("DMLChangeEvent"),
	}
	err := self.logStore.PutLogs(logGroup)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func NewLoghubSink(conf *LoghubConfig) (*LoghubSink, error) {
	client := &sls.Client{
		Endpoint:        conf.Endpoint,
		AccessKeyID:     conf.AccessID,
		AccessKeySecret: conf.AccessSecret,
	}
	logStore, err := client.GetLogStore(conf.Project, conf.LogStore)
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

	return &LoghubSink{
		logStore: logStore,
		idGen:    node,
		recorder: session,
		recordDB: conf.RecorderDB,
	}, nil
}
