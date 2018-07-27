package binlog

import (
	"github.com/siddontang/go-mysql/canal"
	"strings"
	"github.com/siddontang/go-mysql/schema"
	"reflect"
)

const DATE_FORMAT = "2006-01-02T15:04:05.000+08:00"

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

func parseColumns(columns *[]schema.TableColumn) *map[string]schema.TableColumn {
	metaMap := make(map[string]schema.TableColumn)

	nCol := len(*columns)

	for colId := 0; colId < nCol; colId++ {
		metaMap[(*columns)[colId].Name] = (*columns)[colId]
	}
	return &metaMap
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
		var columnChanged []string

		for i := 0; i < len(e.Rows); i+=2 {
			pre := e.Rows[i]
			post := e.Rows[i+1]

			beforeUpdate := *parseRowMap(&e.Table.Columns, pre)
			afterUpdate := *parseRowMap(&e.Table.Columns, post)

			if len(columnChanged) == 0 {
				for col := range afterUpdate {
					if afterUpdate[col] == nil || reflect.TypeOf(afterUpdate[col]).Comparable() {
						if afterUpdate[col] != beforeUpdate[col] {
							columnChanged = append(columnChanged, col)
						}
					} else {
						if !reflect.DeepEqual(afterUpdate[col], beforeUpdate[col]) {
							columnChanged = append(columnChanged, col)
						}
					}
				}
			}

			rowChanges = append(rowChanges, &RowChange{
				BeforeUpdate: beforeUpdate,
				AfterUpdate: afterUpdate,
				ColumnsChanged: columnChanged,
			})
		}
	}

	payload := &DMLPayload{
		EventType: strings.ToUpper(e.Action),
		Db: e.Table.Schema,
		Table: e.Table.Name,
		PKColumn: e.Table.GetPKColumn(0).Name,
		Columns: *parseColumns(&e.Table.Columns),
		Data: rowChanges,
	}
	return payload
}
