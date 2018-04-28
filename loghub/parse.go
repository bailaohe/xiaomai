package loghub

import (
	"github.com/siddontang/go-mysql/canal"
	"strings"
	"github.com/siddontang/go-mysql/schema"
	"time"
	"encoding/json"
	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
	"strconv"
)

const LOGHUB_DATE_FORMAT = "2006-01-02T15:04:05.000+08:00"

type DMLPayload struct {
	EventType string `json:"eventType"`
	Db string	`json:"db"`
	Table string `json:"table"`
	LogFile string `json:"logFile"`
	LogFileOffset string `json:"logfileOffset"`
	Ts uint8 `json:"ts"`
	Data []*RowChange `json:"data"`
}

type RowChange struct {
	BeforeUpdate map[string]interface{} `json:"beforeUpdate"`
	AfterUpdate map[string]interface{} `json:"afterUpdate"`

	ColumnsChanged []string `json:"columnsChanged"`
}

func parseRowMap(columns *[]schema.TableColumn, row []interface{}) *map[string]interface{}{
	rowMap := make(map[string]interface{})
	for colId, colVal := range row {
		rowMap[(*columns)[colId].Name] = colVal
	}
	return &rowMap
}

func ParsePayload(e *canal.RowsEvent) *DMLPayload {
	rowChanges := []*RowChange{}
	if e.Action == canal.InsertAction {
		for _, row := range e.Rows {
			rowChanges = append(rowChanges, &RowChange{
				BeforeUpdate: map[string]interface{}{},
				AfterUpdate: *parseRowMap(&e.Table.Columns, row),
				ColumnsChanged: []string{},
			})
		}
	} else if e.Action == canal.DeleteAction {
		for _, row := range e.Rows {
			rowChanges = append(rowChanges, &RowChange{
				BeforeUpdate: *parseRowMap(&e.Table.Columns, row),
				AfterUpdate: map[string]interface{}{},
				ColumnsChanged: []string{},
			})
		}
	} else if e.Action == canal.UpdateAction {
		for i := 0; i < len(e.Rows); i+=2 {
			pre := e.Rows[i]
			post := e.Rows[i+1]

			rowChanges = append(rowChanges, &RowChange{
				BeforeUpdate: *parseRowMap(&e.Table.Columns, pre),
				AfterUpdate: *parseRowMap(&e.Table.Columns, post),
				ColumnsChanged: []string{},
			})
		}
	}

	payload := &DMLPayload{
		EventType: strings.ToUpper(e.Action),
		Db: e.Table.Schema,
		Table: e.Table.Name,
		Data: rowChanges,
	}
	return payload
}

func ParseLogGroup(e *canal.RowsEvent) (*sls.LogGroup, error){
	now := time.Now()

	payload := ParsePayload(e)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	logGroup := &sls.LogGroup{
		Logs: []*sls.Log{
			{
				Time: proto.Uint32(uint32(now.Unix())),
				Contents: []*sls.LogContent{
					{Key: proto.String("id"), Value: proto.String(strconv.Itoa(int(now.Unix())))},
					{Key: proto.String("level"), Value: proto.String("EVENT")},
					{Key: proto.String("@timestamp"), Value: proto.String(now.Format(LOGHUB_DATE_FORMAT))},
					{Key: proto.String("payload"), Value: proto.String(string(payloadBytes))},
				},
			},
		},
		Source: proto.String(""),
		Topic: proto.String("DMLChangeEvent"),
	}
	return logGroup, nil
}