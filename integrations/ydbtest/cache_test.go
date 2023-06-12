// !datbeohbbh! this test is copied from original xorm tests.
package ydb

import (
	"testing"
	"time"

	"xorm.io/xorm/caches"

	"github.com/stretchr/testify/assert"
)

func TestCacheFind(t *testing.T) {
	type MailBox struct {
		Id       int64 `xorm:"pk"`
		Username string
		Password string
	}

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	oldCacher := engine.GetDefaultCacher()
	cacher := caches.NewLRUCacher2(caches.NewMemoryStore(), time.Hour, 10000)
	engine.SetDefaultCacher(cacher)

	assert.NoError(t, engine.Sync(new(MailBox)))
	defer func() {
		assert.NoError(t, engine.DropTables(new(MailBox)))
	}()

	var inserts = []*MailBox{
		{
			Id:       0,
			Username: "user1",
			Password: "pass1",
		},
		{
			Id:       1,
			Username: "user2",
			Password: "pass2",
		},
	}
	_, err = engine.Insert(inserts[0], inserts[1])
	assert.NoError(t, err)

	var boxes []MailBox
	assert.NoError(t, engine.Find(&boxes))
	assert.EqualValues(t, 2, len(boxes))
	for i, box := range boxes {
		assert.Equal(t, inserts[i].Id, box.Id)
		assert.Equal(t, inserts[i].Username, box.Username)
		assert.Equal(t, inserts[i].Password, box.Password)
	}

	boxes = make([]MailBox, 0, 2)
	assert.NoError(t, engine.Find(&boxes))
	assert.EqualValues(t, 2, len(boxes))
	for i, box := range boxes {
		assert.Equal(t, inserts[i].Id, box.Id)
		assert.Equal(t, inserts[i].Username, box.Username)
		assert.Equal(t, inserts[i].Password, box.Password)
	}

	boxes = make([]MailBox, 0, 2)
	assert.NoError(t, engine.Alias("a").Where("`a`.`id`> -1").
		Asc("`a`.`id`").Find(&boxes))
	assert.EqualValues(t, 2, len(boxes))
	for i, box := range boxes {
		assert.Equal(t, inserts[i].Id, box.Id)
		assert.Equal(t, inserts[i].Username, box.Username)
		assert.Equal(t, inserts[i].Password, box.Password)
	}

	type MailBox4 struct {
		Id       int64
		Username string
		Password string
	}

	boxes2 := make([]MailBox4, 0, 2)
	assert.NoError(t, engine.Table("mail_box").Where("`mail_box`.`id` > -1").
		Asc("mail_box.id").Find(&boxes2))
	assert.EqualValues(t, 2, len(boxes2))
	for i, box := range boxes2 {
		assert.Equal(t, inserts[i].Id, box.Id)
		assert.Equal(t, inserts[i].Username, box.Username)
		assert.Equal(t, inserts[i].Password, box.Password)
	}

	engine.SetDefaultCacher(oldCacher)
}

func TestCacheFind2(t *testing.T) {
	type MailBox2 struct {
		Id       uint64 `xorm:"pk"`
		Username string
		Password string
	}

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	oldCacher := engine.GetDefaultCacher()
	cacher := caches.NewLRUCacher2(caches.NewMemoryStore(), time.Hour, 10000)
	engine.SetDefaultCacher(cacher)

	assert.NoError(t, engine.Sync(new(MailBox2)))
	defer func() {
		assert.NoError(t, engine.DropTables(new(MailBox2)))
	}()

	var inserts = []*MailBox2{
		{
			Id:       0,
			Username: "user1",
			Password: "pass1",
		},
		{
			Id:       1,
			Username: "user2",
			Password: "pass2",
		},
	}
	_, err = engine.Insert(inserts[0], inserts[1])
	assert.NoError(t, err)

	var boxes []MailBox2
	assert.NoError(t, engine.Find(&boxes))
	assert.EqualValues(t, 2, len(boxes))
	for i, box := range boxes {
		assert.Equal(t, inserts[i].Id, box.Id)
		assert.Equal(t, inserts[i].Username, box.Username)
		assert.Equal(t, inserts[i].Password, box.Password)
	}

	boxes = make([]MailBox2, 0, 2)
	assert.NoError(t, engine.Find(&boxes))
	assert.EqualValues(t, 2, len(boxes))
	for i, box := range boxes {
		assert.Equal(t, inserts[i].Id, box.Id)
		assert.Equal(t, inserts[i].Username, box.Username)
		assert.Equal(t, inserts[i].Password, box.Password)
	}

	engine.SetDefaultCacher(oldCacher)
}

func TestCacheGet(t *testing.T) {
	type MailBox3 struct {
		Id       uint64 `xorm:"pk"`
		Username string
		Password string
	}

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	oldCacher := engine.GetDefaultCacher()
	cacher := caches.NewLRUCacher2(caches.NewMemoryStore(), time.Hour, 10000)
	engine.SetDefaultCacher(cacher)

	assert.NoError(t, engine.Sync(new(MailBox3)))
	defer func() {
		assert.NoError(t, engine.DropTables(new(MailBox3)))
	}()

	var inserts = []*MailBox3{
		{
			Id:       0,
			Username: "user1",
			Password: "pass1",
		},
	}
	_, err = engine.Insert(inserts[0])
	assert.NoError(t, err)

	var box1 MailBox3
	has, err := engine.Where("`id` = ?", inserts[0].Id).Get(&box1)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, "user1", box1.Username)
	assert.EqualValues(t, "pass1", box1.Password)

	var box2 MailBox3
	has, err = engine.Where("`id` = ?", inserts[0].Id).Get(&box2)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, "user1", box2.Username)
	assert.EqualValues(t, "pass1", box2.Password)

	engine.SetDefaultCacher(oldCacher)
}
