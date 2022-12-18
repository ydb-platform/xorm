package ydb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
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
	UserID  sql.NullInt64 `xorm:"pk 'user_id'"`
	Number  string        `xorm:"pk 'number'"`
	Created time.Time     `xorm:"created 'created_at'"`
	Updated time.Time     `xorm:"updated 'updated_at'"`
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

func getUsersData() (users []*Users) {
	for i := 0; i < 20; i++ {
		users = append(users, &Users{
			Name: fmt.Sprintf("Dat - %d", i),
			Age:  uint32(22 + i),
			Account: Account{
				UserID: sql.NullInt64{Int64: int64(i), Valid: true},
				Number: uuid.NewString(),
			},
		})
	}
	return
}
