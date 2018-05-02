package loghub

import (
	"github.com/siddontang/go-mysql/canal"
	"strings"
	"github.com/siddontang/go-mysql/schema"
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

	nCol := len(*columns)
	if len(row) < nCol {
		nCol = len(row)
	}

	for colId := 0; colId < nCol; colId++ {
		rowMap[(*columns)[colId].Name] = row[colId]
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
