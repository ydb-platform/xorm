package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
	"xorm.io/xorm"
	"xorm.io/xorm/retry"
)

func TestSelectView(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Series{}))

	engine, err := enginePool.GetScriptQueryEngine()
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

	engine, err := enginePool.GetScriptQueryEngine()
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

	engine, err = enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

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
