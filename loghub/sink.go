package loghub

import (
	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/siddontang/go-mysql/canal"
	"time"
	"encoding/json"
	"github.com/gogo/protobuf/proto"
	"strconv"
	"github.com/juju/errors"
)

type LoghubSink struct {
	logStore *sls.LogStore
}

func (self *LoghubSink) Parse(e *canal.RowsEvent) ([]interface{}, error) {
	now := time.Now()

	payload := ParsePayload(e)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	logs := []interface{}{
		&sls.Log{
			Time: proto.Uint32(uint32(now.Unix())),
			Contents: []*sls.LogContent{
				{Key: proto.String("id"), Value: proto.String(strconv.Itoa(int(now.Unix())))},
				{Key: proto.String("level"), Value: proto.String("EVENT")},
				{Key: proto.String("@timestamp"), Value: proto.String(now.Format(LOGHUB_DATE_FORMAT))},
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

	return &LoghubSink{
		logStore: logStore,
	}, nil
}
