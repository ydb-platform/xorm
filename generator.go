package xorm

import (
	"time"
)

type IDGeneratorInterface interface {
	CreateIdTable() error
	GenNextID() (uint64, error)
	Close() error
}

type generator struct {
	engine         *Engine
	driverName     string
	dataSourceName string
}

type IdTable struct {
	Id      uint64    `xorm:"'id' pk autoincr"`
	GenTime time.Time `xorm:"'gen_time' index(time)"`
}

func NewGeneratorEngine(driverName, dataSourceName string) (IDGeneratorInterface, error) {
	engine, err := NewEngine(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &generator{
		engine:         engine,
		driverName:     driverName,
		dataSourceName: dataSourceName,
	}, nil
}

func (g *generator) CreateIdTable() error {
	err := g.engine.CreateTables(&IdTable{})
	return err
}

func (g *generator) GenNextID() (uint64, error) {
	nxtId := IdTable{GenTime: time.Now()}
	_, err := g.engine.Insert(&nxtId)
	if err != nil {
		return 0, err
	}
	return uint64(nxtId.Id), nil
}

func (g *generator) Close() error {
	err := g.engine.Close()
	return err
}
