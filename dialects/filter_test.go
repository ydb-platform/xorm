package dialects

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeqFilter(t *testing.T) {
	kases := map[string]string{
		"SELECT * FROM TABLE1 WHERE a=? AND b=?":                               "SELECT * FROM TABLE1 WHERE a=$1 AND b=$2",
		"SELECT 1, '???', '2006-01-02 15:04:05' FROM TABLE1 WHERE a=? AND b=?": "SELECT 1, '???', '2006-01-02 15:04:05' FROM TABLE1 WHERE a=$1 AND b=$2",
		"select '1''?' from issue":                                             "select '1''?' from issue",
		"select '1\\??' from issue":                                            "select '1\\??' from issue",
		"select '1\\\\',? from issue":                                          "select '1\\\\',$1 from issue",
		"select '1\\''?',? from issue":                                         "select '1\\''?',$1 from issue",
	}
	for sql, result := range kases {
		assert.EqualValues(t, result, postgresSeqFilterConvertQuestionMark(sql, "$", 1))
	}
}

func TestSeqFilterLineComment(t *testing.T) {
	kases := map[string]string{
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
		assert.EqualValues(t, result, postgresSeqFilterConvertQuestionMark(sql, "$", 1))
	}
}

func TestSeqFilterComment(t *testing.T) {
	kases := map[string]string{
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
		assert.EqualValues(t, result, postgresSeqFilterConvertQuestionMark(sql, "$", 1))
	}
}

func TestSeqFilterPostgresQuestions(t *testing.T) {
	kases := map[string]string{
		`SELECT '{"a":1, "b":2}'::jsonb ? 'b'
		FROM table1 WHERE c = ?`: `SELECT '{"a":1, "b":2}'::jsonb ? 'b'
		FROM table1 WHERE c = $1`,
	}
	for sql, result := range kases {
		assert.EqualValues(t, result, postgresSeqFilterConvertQuestionMark(sql, "$", 1))
	}
}
