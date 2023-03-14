package ydb

import (
	"testing"
)

type UpsertUserA struct {
	Uuid int64 `xorm:"pk"`
	Msg  string
	Age  uint32
}

func (*UpsertUserA) TableName() string {
	return "upsert_user_a"
}

type UpsertUserB struct {
	UpsertUserA `xorm:"extends"`
}

func (*UpsertUserB) TableName() string {
	return "test/upsert_user_b"
}

func TestUpsertSinglePK(t *testing.T) {
	t.Skip()
}

func TestUpsertSinglePKByFetch(t *testing.T) {
	t.Skip()
}

type UpsertUsers struct {
	Users `xorm:"extends"`
}

func (*UpsertUsers) TableName() string {
	return "upsert_users"
}

func TestUpsertCompositePK(t *testing.T) {
	t.Skip()
}
