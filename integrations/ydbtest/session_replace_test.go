package ydb

import (
	"testing"
)

type ReplaceUserA struct {
	Uuid int64 `xorm:"pk"`
	Msg  string
	Age  uint32
}

func (*ReplaceUserA) TableName() string {
	return "replace_user_a"
}

type ReplaceUserB struct {
	ReplaceUserA `xorm:"extends"`
}

func (*ReplaceUserB) TableName() string {
	return "test/replace_user_b"
}

func TestReplaceSinglePK(t *testing.T) {
	t.Skip()
}

func TestReplaceSinglePKByFetch(t *testing.T) {
	t.Skip()
}

type ReplaceUsers struct {
	Users `xorm:"extends"`
}

func (*ReplaceUsers) TableName() string {
	return "replace_users"
}

func TestReplaceCompositePK(t *testing.T) {
	t.Skip()
}

func TestReplaceCompositePKDefaultValues(t *testing.T) {
	t.Skip()
}
