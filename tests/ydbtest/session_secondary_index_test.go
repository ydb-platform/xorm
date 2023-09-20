package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
	"xorm.io/xorm"
	"xorm.io/xorm/dialects"
	"xorm.io/xorm/retry"
)

func TestSelectView(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Series{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	series, _, _ := getData()
	err = engine.DoTx(enginePool.ctx, func(ctx context.Context, session *xorm.Session) error {
		_, err := session.Insert(&series)
		return err
	},
		retry.WithID(t.Name()),
		retry.WithIdempotent(true))

	assert.NoError(t, err)

	yql, _, err := builder.Select("COUNT(*)").From((&Series{}).TableName() + " VIEW index_series_title").ToSQL()
	assert.NoError(t, err)

	series_title, err := engine.SQL(yql).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, len(series), series_title)
}

func TestViewCond(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Seasons{}, &Episodes{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	_, seasons, episodes := getData()
	err = engine.DoTx(enginePool.ctx, func(ctx context.Context, session *xorm.Session) error {
		_, err := session.Insert(&seasons, &episodes)
		return err
	},
		retry.WithID(t.Name()),
		retry.WithIdempotent(true))

	assert.NoError(t, err)

	t.Run("query-view", func(t *testing.T) {
		var season Seasons
		t.Run("get-season", func(t *testing.T) {
			session := engine.NewSession()
			defer session.Close()

			// data: Silicon Valley - season 1
			yql, args, err := builder.
				Select("season_id, title, first_aired, last_aired").
				From((&Seasons{}).TableName() + " VIEW index_season_first_aired").
				Where(builder.Eq{
					"first_aired": date("2014-04-06"),
				}).
				ToSQL()
			assert.NoError(t, err)

			has, err := session.SQL(yql, args...).Get(&season)

			assert.NoError(t, err)
			assert.True(t, has)

			t.Log(season)
		})

		t.Run("count-episodes", func(t *testing.T) {
			session := engine.NewSession()
			defer session.Close()

			// data: count episodes of Silicon Valley - season 1
			// expected: 8
			yql, args, err := builder.
				Select("COUNT(*)").
				From((&Episodes{}).TableName() + " VIEW index_episodes_air_date").
				Where(builder.Between{
					Col:     "air_date",
					LessVal: season.FirstAired,
					MoreVal: season.LastAired,
				}).
				ToSQL()
			assert.NoError(t, err)

			cnt, err := session.SQL(yql, args...).Count()

			assert.NoError(t, err)
			assert.EqualValues(t, 8, cnt)
		})

		t.Run("get-episodes", func(t *testing.T) {
			session := engine.NewSession()
			defer session.Close()

			var episodeData []Episodes
			err := session.
				Table((&Episodes{}).TableName()).
				Where("season_id = ?", season.SeasonID).
				Find(&episodeData)
			assert.NoError(t, err)

			// data: get episodes of Silicon Valley - season 1
			yql, args, err := builder.
				Select("*").
				From((&Episodes{}).TableName() + " VIEW index_episodes_air_date").
				Where(builder.Between{
					Col:     "air_date",
					LessVal: season.FirstAired,
					MoreVal: season.LastAired,
				}).
				ToSQL()
			assert.NoError(t, err)

			var res []Episodes
			err = session.SQL(yql, args...).Find(&res)

			assert.NoError(t, err)
			assert.ElementsMatch(t, episodeData, res)
		})
	})
}

func TestJoinView(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Series{}, &Seasons{}, &Episodes{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	series, seasons, episodes := getData()
	err = engine.DoTx(enginePool.ctx, func(ctx context.Context, session *xorm.Session) error {
		_, err := session.Insert(&series, &seasons, &episodes)
		return err
	},
		retry.WithID(t.Name()),
		retry.WithIdempotent(true))

	assert.NoError(t, err)

	type JoinResult struct {
		SeriesID   []byte `xorm:"'series_id'"`
		SeasonID   []byte `xorm:"'season_id'"`
		Title      string `xorm:"'title'"`
		SeriesInfo string `xorm:"'series_info'"`
	}

	session := engine.NewSession()
	defer session.Close()

	session.Engine().SetQuotePolicy(dialects.QuotePolicyNone)
	defer session.Engine().SetQuotePolicy(dialects.QuotePolicyAlways)

	res := make([]JoinResult, 0)

	err = session.
		Table(&Seasons{}).
		Alias("ss").
		Select("ss.series_id as series_id, ss.season_id as season_id, ss.title as title, se.series_info as series_info").
		Join("LEFT", []string{(&Series{}).TableName() + " VIEW index_series_title", "se"}, "ss.title = se.title").
		Find(&res)
	assert.NoError(t, err)

	assert.EqualValues(t, len(seasons), len(res))
}

func TestJoinViewCond(t *testing.T) {
	type A struct {
		Id   int64 `xorm:"pk 'id'"`
		ColA int64 `xorm:"'col_a' index(index_col_a)"`
	}

	type B struct {
		Id   int64 `xorm:"pk 'id'"`
		ColB int64 `xorm:"'col_b' index(index_col_b)"`
	}

	assert.NoError(t, PrepareScheme(&A{}, &B{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	for i := 1; i <= 10; i++ {
		_, err = session.Insert(&A{Id: int64(i), ColA: int64(i)}, &B{Id: int64(i), ColB: int64(i)})
		assert.NoError(t, err)
	}

	session.Engine().SetQuotePolicy(dialects.QuotePolicyNone)
	defer session.Engine().SetQuotePolicy(dialects.QuotePolicyAlways)

	type Result struct {
		Id  int64 `xorm:"'id'"`
		Col int64 `xorm:"'col'"`
	}

	res := make([]Result, 0)

	err = session.
		Table("a VIEW index_col_a").
		Alias("table_a").
		Select("table_a.id as id, table_a.col_a as col").
		Join("INNER", []string{"b VIEW index_col_b", "table_b"}, "table_a.col_a = table_b.col_b").
		Where("table_a.col_a >= ?", 5).
		Asc("id").
		Find(&res)

	assert.NoError(t, err)
	assert.EqualValues(t, 6, len(res))

	t.Log(res)
	t.Log(session.LastSQL())

	for i := 0; i < len(res); i++ {
		assert.EqualValues(t, Result{Id: int64(i + 5), Col: int64(i + 5)}, res[i])
	}
}
