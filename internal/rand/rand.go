package rand

import (
	"math/rand"
	"reflect"
	"time"

	"github.com/google/uuid"
)

func RandValue(val reflect.Value) interface{} {
	switch val.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int, reflect.Int64:
		rand.New(rand.NewSource(time.Now().UnixNano()))
		return rand.Int63() >> 11
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint, reflect.Uint64:
		rand.New(rand.NewSource(time.Now().UnixNano()))
		return rand.Uint64() >> 11
	case reflect.String:
		return uuid.NewString()
	default:
		return []byte(uuid.NewString())
	}
}
