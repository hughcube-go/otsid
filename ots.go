package otsid

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"sort"
	"strings"
	"sync"
	"time"
)

type OtsId struct {
	TableName   string
	DefaultType string
	Ots         *tablestore.TableStoreClient
}

type ids []int64

func (p ids) Len() int           { return len(p) }
func (p ids) Less(i, j int) bool { return p[i] < p[j] }
func (p ids) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (o *OtsId) One(types ...string) (int64, error) {
	if 0 >= len(types) && 0 < len(o.DefaultType) {
		types = append(types, o.DefaultType)
	}

	if 0 >= len(types) {
		types = append(types, "default")
	}

	request := new(tablestore.PutRowRequest)

	request.PutRowChange = new(tablestore.PutRowChange)
	request.PutRowChange.TableName = o.TableName

	request.PutRowChange.PrimaryKey = new(tablestore.PrimaryKey)
	request.PutRowChange.PrimaryKey.AddPrimaryKeyColumn("type", strings.Join(types, ","))
	request.PutRowChange.PrimaryKey.AddPrimaryKeyColumnWithAutoIncrement("id")
	request.PutRowChange.AddColumn("created_at", time.Now().Format(time.RFC3339Nano))
	request.PutRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	request.PutRowChange.SetReturnPk()

	response, err := o.Ots.PutRow(request)

	if err != nil {
		return 0, err
	}

	var id int64
	for _, v := range response.PrimaryKey.PrimaryKeys {
		if "id" == v.ColumnName {
			id = v.Value.(int64)
		}
	}

	if 0 >= id {
		return id, errors.New("id not find")
	}

	return id, nil
}

func (o *OtsId) Batch(count int, types ...string) ([]int64, error) {
	var ids ids
	var err error

	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)
	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, er := o.One(types...)

			mu.Lock()
			defer mu.Unlock()
			if er == nil {
				ids = append(ids, id)
			} else {
				err = er
			}
		}()
	}
	wg.Wait()
	sort.Sort(ids)

	return ids, err
}
