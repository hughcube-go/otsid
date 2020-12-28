package otsid

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"strings"
	"sync"
)

type OtsId struct {
	EndPoint        string
	InstanceName    string
	AccessKeyId     string
	AccessKeySecret string

	TableName      string
	TypeColumnName string
	IdColumnName   string

	DefaultType string

	otsClient     *tablestore.TableStoreClient
	otsClientOnce *sync.Once
}

type Config interface {
	GetEndPoint() string
	GetInstanceName() string
	GetAccessKeyId() string
	GetAccessKeySecret() string

	GetTableName() string
	GetTypeColumnName() string
	GetIdColumnName() string

	GetDefaultType() string
}

func New(config Config) *OtsId {
	client := &OtsId{
		EndPoint:        config.GetEndPoint(),
		InstanceName:    config.GetInstanceName(),
		AccessKeyId:     config.GetAccessKeyId(),
		AccessKeySecret: config.GetAccessKeySecret(),
		TableName:       config.GetTableName(),
		TypeColumnName:  config.GetTypeColumnName(),
		IdColumnName:    config.GetIdColumnName(),
		DefaultType:     config.GetDefaultType(),
	}

	if "" == client.DefaultType {
		client.DefaultType = "default"
	}

	return client
}

func (o *OtsId) getOtsClient() *tablestore.TableStoreClient {
	o.otsClientOnce.Do(func() {
		if o.otsClient != nil {
			return
		}

		o.otsClient = tablestore.NewClient(
			o.EndPoint,
			o.InstanceName,
			o.AccessKeyId,
			o.AccessKeySecret,
		)
	})

	return o.otsClient
}

func (o *OtsId) GetId(types ...string) (int64, error) {
	if 0 >= len(types) {
		types = append(types, o.DefaultType)
	}

	request := new(tablestore.PutRowRequest)

	request.PutRowChange = new(tablestore.PutRowChange)
	request.PutRowChange.TableName = o.TableName

	request.PutRowChange.PrimaryKey = new(tablestore.PrimaryKey)
	request.PutRowChange.PrimaryKey.AddPrimaryKeyColumn(o.TypeColumnName, strings.Join(types, ","))
	request.PutRowChange.PrimaryKey.AddPrimaryKeyColumnWithAutoIncrement(o.IdColumnName)
	request.PutRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	request.PutRowChange.SetReturnPk()

	response, err := o.getOtsClient().PutRow(request)

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
