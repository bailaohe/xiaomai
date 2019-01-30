package binlog

import (
	"gopkg.in/mgo.v2/bson"
	"time"
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
		Event: "DBSyncEvent",
		Topic: "DBSyncEvent",
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
