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
	"gopkg.in/mgo.v2/bson"
)

const (
	MYSQL_SYNC_SERVICE_ID = 11
	RECORDER_COLLECTION = "SysEventRecord"
)

type EventRecord struct {
	ID bson.ObjectId `bson:"_id"`
	Event string `bson:"event"`
	Topic string `bson:"topic"`

	GmtCreate time.Time `bson:"gmt_create"`
	GmtModify time.Time `bson:"gmt_modify"`

	Status int `bson:"status"`

	Message string `bson:"message"`

	OperatorId int64 `bson:"operator_id"`
	ModalType int `bson:"modal_type"`

	Title string  `bson:"title"`
	Content string  `bson:"content"`

	ExecCount int `bson:"execCount"`
	SuccessCount int `bson:"successCount"`
	FailCount int `bson:"failCount"`
	RetryCount int `bson:"retryCount"`
}

func NewEventRecord(now time.Time, content string) *EventRecord {
	return &EventRecord{
		ID: bson.NewObjectId(),
		Event: "DMLChangeEvent",
		Topic: "DMLChangeEvent",
		GmtCreate: now,
		GmtModify: now,
		Status: 0,
		OperatorId: 0,
		ModalType: -1,
		Title: "Data Modified",
		ExecCount: 0,
		SuccessCount: 0,
		FailCount: 0,
		RetryCount: 0,
		Content: content,
	}
}

type LoghubSink struct {
	logStore *sls.LogStore
	idGen *snowflake.Node
	recorder *mgo.Session
	recordDB string
}

func (self *LoghubSink) Parse(e *canal.RowsEvent) ([]interface{}, error) {
	now := time.Now()

	payload := ParsePayload(e)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	eventRecord := NewEventRecord(now, string(payloadBytes))
	err = self.recorder.DB(self.recordDB).C(RECORDER_COLLECTION).Insert(eventRecord)
	if err != nil {
		return nil, err
	}

	logs := []interface{}{
		&sls.Log{
			Time: proto.Uint32(uint32(now.Unix())),
			Contents: []*sls.LogContent{
				{Key: proto.String("id"), Value: proto.String(eventRecord.ID.Hex())},
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

	node, err := snowflake.NewNode(MYSQL_SYNC_SERVICE_ID)
	if err != nil {
		return nil, err
	}

	session, err := mgo.Dial(conf.RecorderAddr)
	if err != nil {
		return nil, err
	}

	err = session.Login(&mgo.Credential{
		Username: conf.RecorderUser,
		Password: conf.RecorderPass,
	})

	return &LoghubSink{
		logStore: logStore,
		idGen: node,
		recorder: session,
		recordDB: conf.RecorderDB,
	}, nil
}
