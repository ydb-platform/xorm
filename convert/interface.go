// Copyright 2021 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package convert

import (
	"database/sql"
	"fmt"
	"time"
)

// Interface2Interface converts interface of pointer as interface of value
func Interface2Interface(userLocation *time.Location, v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch vv := v.(type) {
	case *int64:
		return *vv, nil
	case *int8:
		return *vv, nil
	case *sql.NullString:
		return vv.String, nil
	case *sql.RawBytes:
		if len([]byte(*vv)) > 0 {
			return []byte(*vv), nil
		}
		return nil, nil
	case *sql.NullInt32:
		return vv.Int32, nil
	case *sql.NullInt64:
		return vv.Int64, nil
	case *sql.NullFloat64:
		return vv.Float64, nil
	case *sql.NullBool:
		if vv.Valid {
			return vv.Bool, nil
		}
		return nil, nil
	case *sql.NullTime:
		if vv.Valid {
			return vv.Time.In(userLocation).Format("2006-01-02 15:04:05"), nil
		}
		return "", nil
	// !datbeohbbh! only YDB?: new type that implement scanner interface
	case *NullInt8:
		if vv.Valid {
			return vv.Int8, nil
		}
		return nil, nil
	case *NullUint16:
		if vv.Valid {
			return vv.Uint16, nil
		}
		return nil, nil
	case *NullUint32:
		if vv.Valid {
			return vv.Uint32, nil
		}
		return nil, nil
	case *NullUint64:
		if vv.Valid {
			return vv.Uint64, nil
		}
		return nil, nil
	case *NullFloat32:
		if vv.Valid {
			return vv.Float32, nil
		}
		return nil, nil
	default:
		return "", fmt.Errorf("convert assign string unsupported type: %#v", vv)
	}
}
