package dialects

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeqFilter(t *testing.T) {
	var kases = map[string]string{
		"SELECT * FROM TABLE1 WHERE a=? AND b=?":                               "SELECT * FROM TABLE1 WHERE a=$1 AND b=$2",
		"SELECT 1, '???', '2006-01-02 15:04:05' FROM TABLE1 WHERE a=? AND b=?": "SELECT 1, '???', '2006-01-02 15:04:05' FROM TABLE1 WHERE a=$1 AND b=$2",
		"select '1''?' from issue":                                             "select '1''?' from issue",
		"select '1\\??' from issue":                                            "select '1\\??' from issue",
		"select '1\\\\',? from issue":                                          "select '1\\\\',$1 from issue",
		"select '1\\''?',? from issue":                                         "select '1\\''?',$1 from issue",
	}
	for sql, result := range kases {
		assert.EqualValues(t, result, convertQuestionMark(sql, "$", 1))
	}
}

func TestSeqFilterLineComment(t *testing.T) {
	var kases = map[string]string{
		`SELECT *
		FROM TABLE1
		WHERE foo='bar'
		AND a=? -- it's a comment
		AND b=?`: `SELECT *
		FROM TABLE1
		WHERE foo='bar'
		AND a=$1 -- it's a comment
		AND b=$2`,
		`SELECT *
		FROM TABLE1
		WHERE foo='bar'
		AND a=? -- it's a comment?
		AND b=?`: `SELECT *
		FROM TABLE1
		WHERE foo='bar'
		AND a=$1 -- it's a comment?
		AND b=$2`,
		`SELECT *
		FROM TABLE1
		WHERE a=? -- it's a comment? and that's okay?
		AND b=?`: `SELECT *
		FROM TABLE1
		WHERE a=$1 -- it's a comment? and that's okay?
		AND b=$2`,
	}
	for sql, result := range kases {
		assert.EqualValues(t, result, convertQuestionMark(sql, "$", 1))
	}
}

func TestSeqFilterComment(t *testing.T) {
	var kases = map[string]string{
		`SELECT *
		FROM TABLE1
		WHERE a=? /* it's a comment */
		AND b=?`: `SELECT *
		FROM TABLE1
		WHERE a=$1 /* it's a comment */
		AND b=$2`,
		`SELECT /* it's a comment * ?
		More comment on the next line! */ *
		FROM TABLE1
		WHERE a=? /**/
		AND b=?`: `SELECT /* it's a comment * ?
		More comment on the next line! */ *
		FROM TABLE1
		WHERE a=$1 /**/
		AND b=$2`,
	}
	for sql, result := range kases {
		assert.EqualValues(t, result, convertQuestionMark(sql, "$", 1))
	}
}

func TestSeqFilterForYdb(t *testing.T) {
	var cases = map[string]string{
		`SELECT season_id FROM seasons WHERE title LIKE ? AND views > ?`:             `DECLARE $param_1 AS Utf8;DECLARE $param_2 AS Uint64;SELECT season_id FROM seasons WHERE title LIKE $param_1 AND views > $param_2`,
		`SELECT season_id FROM seasons WHERE title LIKE ? AND views > ? /* ????? */`: `DECLARE $param_1 AS Utf8;DECLARE $param_2 AS Uint64;SELECT season_id FROM seasons WHERE title LIKE $param_1 AND views > $param_2 /* ????? */`,
	}

	yf := &SeqFilter{
		Prefix: "$param_",
		Start:  1,
	}

	namedValue := []interface{}{
		sql.Named("seasonTitle", "%Season 1%"),
		sql.Named("views", uint64(1000)),
	}
	for sqlStr, result := range cases {
		declareSection := yf.GenerateDeclareSection(namedValue...)
		actual := declareSection + yf.Do(sqlStr)
		assert.EqualValues(t, result, actual)
	}
}
