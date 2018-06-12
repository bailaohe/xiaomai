package data

import "github.com/siddontang/go-mysql/schema"

type DMLPayload struct {
	EventType string `json:"eventType"`
	Db string	`json:"db"`
	Table string `json:"table"`
	PKColumn string `json:"pkColumn"`
	LogFile string `json:"logFile"`
	LogFileOffset string `json:"logfileOffset"`
	Ts uint8 `json:"ts"`
	Columns map[string]schema.TableColumn `json:"columns"`
	Data []*RowChange `json:"data"`
}

type RowChange struct {
	BeforeUpdate map[string]interface{} `json:"beforeUpdate"`
	AfterUpdate map[string]interface{} `json:"afterUpdate"`

	ColumnsChanged []string `json:"columnsChanged"`
}
