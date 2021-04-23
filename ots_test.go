package otsid

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func test_get_otsId() *OtsId {
	ots := tablestore.NewClient(
		os.Getenv("ALIYUN_OTS_END_POINT"),
		os.Getenv("ALIYUN_OTS_INSTANCE_NAME"),
		os.Getenv("ALIYUN_ACCESS_KEY"),
		os.Getenv("ALIYUN_ACCESS_KEY_SECRET"),
	)

	return &OtsId{
		TableName:  "msid_source",
		Ots:        ots,
	}
}

func Test_one(t *testing.T) {
	a := assert.New(t)

	id, err := test_get_otsId().One()

	a.Nil(err)
	a.IsType(int64(0), id)
}

func Test_Batch(t *testing.T) {
	a := assert.New(t)

	count := 10
	ids, err := test_get_otsId().Batch(count, "")

	a.Nil(err)
	a.IsType([]int64{}, ids)
	a.Len(ids, count)
}
