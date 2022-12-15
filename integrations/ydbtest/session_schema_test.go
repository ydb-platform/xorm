package ydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	engine, err := GetSchemeQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	if err := session.DropTable(&Users{}); err != nil {
		t.Fatal(err)
	}

	if err := session.CreateTable(&Users{}); err != nil {
		t.Fatal(err)
	}
}
