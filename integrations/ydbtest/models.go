package ydb

import (
	"database/sql"
	"time"
)

type Series struct {
	SeriesID    []byte    `xorm:"pk 'series_id'"`
	Title       string    `xorm:"'title' index(index_series_title)"`
	SeriesInfo  string    `xorm:"'series_info'"`
	ReleaseDate time.Time `xorm:"'release_date'"`
	Comment     string    `xorm:"'comment'"`
}

type Seasons struct {
	SeriesID   []byte    `xorm:"pk 'series_id'"`
	SeasonID   []byte    `xorm:"pk 'season_id'"`
	Title      string    `xorm:"'title' index(index_series_title)"`
	FirstAired time.Time `xorm:"'first_aired' index(index_season_first_aired)"`
	LastAired  time.Time `xorm:"'last_aired'"`
}

type Episodes struct {
	SeriesID  []byte    `xorm:"pk 'series_id'"`
	SeasonID  []byte    `xorm:"pk 'season_id'"`
	EpisodeID []byte    `xorm:"pk 'episode_id'"`
	Title     string    `xorm:"'title'"`
	AirDate   time.Time `xorm:"'air_date' index(index_episodes_air_date)"`
	Views     uint64    `xorm:"'views'"`
}

type TestEpisodes struct {
	Episodes `xorm:"extends"`
}

type Users struct {
	Name    string `xorm:"'name'"`
	Age     uint32 `xorm:"'age'"`
	Account `xorm:"extends"`
}

type Account struct {
	UserID sql.NullInt64 `xorm:"pk 'user_id'"`
	Number string        `xorm:"'number'"`
}

// table name method
func (*Series) TableName() string {
	return "series"
}

func (*Seasons) TableName() string {
	return "seasons"
}

func (*Episodes) TableName() string {
	return "episodes"
}

func (*TestEpisodes) TableName() string {
	return "test_extends_episodes"
}

func (*Users) TableName() string {
	return "users"
}
