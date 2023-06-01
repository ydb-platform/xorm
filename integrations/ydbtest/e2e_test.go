package ydb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"xorm.io/xorm"
	"xorm.io/xorm/retry"

	_ "github.com/ydb-platform/ydb-go-sdk/v3"
)

type e2e struct {
	ctx     context.Context
	engines *EngineWithMode
}

func TestE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	scope := &e2e{
		ctx:     ctx,
		engines: NewEngineWithMode(ctx, connString),
	}

	t.Run("create-engine", func(t *testing.T) {
		engine, err := scope.engines.GetDefaultEngine()
		require.NoError(t, err)

		err = engine.PingContext(ctx)
		require.NoError(t, err)

		err = engine.Close()
		require.NoError(t, err)
	})

	t.Run("e2e", func(t *testing.T) {
		t.Run("prepare-stage", func(t *testing.T) {
			t.Run("scheme", func(t *testing.T) {
				err := scope.prepareScheme()
				require.NoError(t, err)
			})
		})

		t.Run("fill-stage", func(t *testing.T) {
			err := scope.fill()
			require.NoError(t, err)
		})

		t.Run("query-stage", func(t *testing.T) {
			t.Run("explain", func(t *testing.T) {
				engine, err := scope.engines.GetExplainQueryEngine()
				require.NoError(t, err)

				results, err := engine.
					Table(&Episodes{}).
					Cols("views").
					Where("series_id = ?", uuid.New()).
					And("season_id = ?", uuid.New()).
					And("episode_id = ?", uuid.New()).
					QueryString()
				require.NoError(t, err)
				require.Greater(t, len(results), 0)

				explain := results[0]
				ast := explain["AST"]
				plan := explain["Plan"]

				t.Logf("ast = %v\n", ast)
				t.Logf("plan = %v\n", plan)
			})

			t.Run("increment", func(t *testing.T) {
				t.Run("views", func(t *testing.T) {
					engine, err := scope.engines.GetDataQueryEngine()
					require.NoError(t, err)

					err = engine.DoTx(scope.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
						var epData Episodes
						_, err = session.Get(&epData)
						if err != nil {
							return err
						}

						session.
							Table(Episodes{}).
							Cols("views").
							Where("series_id = ?", epData.SeriesID).
							And("season_id = ?", epData.SeasonID).
							And("episode_id = ?", epData.EpisodeID).
							Prepare()

						rows, err := session.Rows(&Episodes{})
						if err != nil {
							return err
						}
						defer func() {
							_ = rows.Close()
						}()

						for rows.Next() {
							var ep Episodes
							if err = rows.Scan(&ep); err != nil {
								return fmt.Errorf("cannot scan views: %w", err)
							}
							t.Logf("got views: %+v\n", ep.Views)

							// increase views by 1
							_, err = session.
								Table(Episodes{}).
								Where("series_id = ?", epData.SeriesID).
								And("season_id = ?", epData.SeasonID).
								And("episode_id = ?", epData.EpisodeID).
								Incr("views", uint64(1)).
								Update(&Episodes{})

							if err != nil {
								return fmt.Errorf("cannot increase views by 1 %w", err)
							}
						}

						return nil
					}, retry.WithID("e2e-test-query-increment"),
						retry.WithIdempotent(true))

					require.NoError(t, err)
				})
			})

			t.Run("select", func(t *testing.T) {
				engine, err := scope.engines.GetScanQueryEngine()
				require.NoError(t, err)

				err = engine.DoTx(scope.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
					has, err := session.
						Table(Episodes{}).
						Where("views = ?", uint64(1)).
						Exist()
					if err != nil {
						return err
					}
					if !has {
						return fmt.Errorf("expected record exists")
					}

					rows, err := session.
						Table(Episodes{}).
						Cols("title", "air_date", "views").
						Where("views = ?", uint64(1)).
						Rows(&Episodes{})

					if err != nil {
						return err
					}
					defer func() {
						_ = rows.Close()
					}()

					for rows.Next() {
						var (
							title    string
							air_date time.Time
							views    uint64
						)
						if err := rows.Scan(&title, &air_date, &views); err != nil {
							return err
						}
						t.Logf("> %v %v %v\n", title, views, air_date.Format("2006-01-02"))
					}

					return nil
				}, retry.WithID("e2e-test-query-select"),
					retry.WithIdempotent(true))

				require.NoError(t, err)
			})
		})

		t.Run("close-engines", func(t *testing.T) {
			err := scope.engines.Close()
			require.NoError(t, err)
		})
	})
}

func (scope *e2e) fill() error {
	engine, err := scope.engines.GetDataQueryEngine()
	if err != nil {
		return err
	}
	series, seasons, episodes := getData()
	err = engine.DoTx(scope.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
		_, err = session.Insert(series, seasons, episodes)
		return err
	},
		retry.WithID("e2e-test-fill-stage"),
		retry.WithIdempotent(true))
	return err
}

func (scope *e2e) prepareScheme() error {
	engine, err := scope.engines.GetSchemeQueryEngine()
	if err != nil {
		return err
	}
	if err = engine.DropTables(&Series{}, &Seasons{}, &Episodes{}); err != nil {
		return err
	}
	if err = engine.CreateTables(&Series{}, &Seasons{}, &Episodes{}); err != nil {
		return err
	}
	return nil
}
