package otsid

import (
	"os"
	"reflect"
	"testing"
)

func TestGetId(t *testing.T) {
	o := &OtsId{
		EndPoint:        os.Getenv("EndPoint"),
		InstanceName:    os.Getenv("InstanceName"),
		AccessKeyId:     os.Getenv("AccessKeyId"),
		AccessKeySecret: os.Getenv("AccessKeySecret"),

		TableName:      os.Getenv("TableName"),
		TypeColumnName: "type",
		IdColumnName:   "id",
	}

	id, err := o.GetId()
	if err != nil {
		panic(err)
	}

	if reflect.TypeOf(id).Kind() != reflect.Int64 || 0 >= id {
		panic(id)
	}

	print(id)
}
