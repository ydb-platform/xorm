package ydb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Series struct {
	SeriesID    []byte       `xorm:"pk 'series_id'"`
	Title       string       `xorm:"'title' index(index_series_title)"`
	SeriesInfo  string       `xorm:"'series_info'"`
	ReleaseDate sql.NullTime `xorm:"'release_date'"`
	Comment     string       `xorm:"'comment'"`
}

type Seasons struct {
	SeriesID   []byte       `xorm:"pk 'series_id'"`
	SeasonID   []byte       `xorm:"pk 'season_id'"`
	Title      string       `xorm:"'title' index(index_series_title)"`
	FirstAired sql.NullTime `xorm:"'first_aired' index(index_season_first_aired)"`
	LastAired  sql.NullTime `xorm:"'last_aired'"`
}

type Episodes struct {
	SeriesID  []byte       `xorm:"pk 'series_id'"`
	SeasonID  []byte       `xorm:"pk 'season_id'"`
	EpisodeID []byte       `xorm:"pk 'episode_id'"`
	Title     string       `xorm:"'title'"`
	AirDate   sql.NullTime `xorm:"'air_date' index(index_episodes_air_date)"`
	Views     uint64       `xorm:"'views'"`
}

type TestEpisodes struct {
	Episodes `xorm:"extends"`
}

type Users struct {
	Name    string `xorm:"'name' INDEX"`
	Age     uint32 `xorm:"'age' INDEX"`
	Account `xorm:"extends"`
}

type Account struct {
	UserID  sql.NullInt64 `xorm:"pk 'user_id'"`
	Number  string        `xorm:"pk 'number'"`
	Created sql.NullTime  `xorm:"'created_at'"`
	Updated sql.NullTime  `xorm:"'updated_at'"`
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
	return "test/episodes"
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

func seriesData(id string, released sql.NullTime, title, info, comment string) *Series {
	return &Series{
		SeriesID:    []byte(id),
		Title:       title,
		SeriesInfo:  info,
		ReleaseDate: released,
		Comment:     comment,
	}
}

func seasonData(seriesID, seasonID string, title string, first, last sql.NullTime) *Seasons {
	return &Seasons{
		SeriesID:   []byte(seriesID),
		SeasonID:   []byte(seasonID),
		Title:      title,
		FirstAired: first,
		LastAired:  last,
	}
}

func episodeData(seriesID, seasonID, episodeID string, title string, date sql.NullTime) *Episodes {
	return &Episodes{
		SeriesID:  []byte(seriesID),
		SeasonID:  []byte(seasonID),
		EpisodeID: []byte(episodeID),
		Title:     title,
		AirDate:   date,
	}
}

func getData() (series []*Series, seasons []*Seasons, episodes []*Episodes) {
	for seriesID, fill := range map[string]func(seriesID string) (seriesData *Series, seasons []*Seasons, episodes []*Episodes){
		uuid.New().String(): getDataForITCrowd,
		uuid.New().String(): getDataForSiliconValley,
	} {
		seriesData, seasonsData, episodesData := fill(seriesID)
		series = append(series, seriesData)
		seasons = append(seasons, seasonsData...)
		episodes = append(episodes, episodesData...)
	}
	return
}

func getDataForITCrowd(seriesID string) (series *Series, seasons []*Seasons, episodes []*Episodes) {
	series = seriesData(
		seriesID, date("2006-02-03"), "IT Crowd", ""+
			"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
			"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
		"", // NULL comment.
	)
	for _, season := range []struct {
		title    string
		first    sql.NullTime
		last     sql.NullTime
		episodes map[string]sql.NullTime
	}{
		{"Season 1", date("2006-02-03"), date("2006-03-03"), map[string]sql.NullTime{
			"Yesterday's Jam":             date("2006-02-03"),
			"Calamity Jen":                date("2006-02-03"),
			"Fifty-Fifty":                 date("2006-02-10"),
			"The Red Door":                date("2006-02-17"),
			"The Haunting of Bill Crouse": date("2006-02-24"),
			"Aunt Irma Visits":            date("2006-03-03"),
		}},
		{"Season 2", date("2007-08-24"), date("2007-09-28"), map[string]sql.NullTime{
			"The Work Outing":            date("2006-08-24"),
			"Return of the Golden Child": date("2007-08-31"),
			"Moss and the German":        date("2007-09-07"),
			"The Dinner Party":           date("2007-09-14"),
			"Smoke and Mirrors":          date("2007-09-21"),
			"Men Without Women":          date("2007-09-28"),
		}},
		{"Season 3", date("2008-11-21"), date("2008-12-26"), map[string]sql.NullTime{
			"From Hell":       date("2008-11-21"),
			"Are We Not Men?": date("2008-11-28"),
			"Tramps Like Us":  date("2008-12-05"),
			"The Speech":      date("2008-12-12"),
			"Friendface":      date("2008-12-19"),
			"Calendar Geeks":  date("2008-12-26"),
		}},
		{"Season 4", date("2010-06-25"), date("2010-07-30"), map[string]sql.NullTime{
			"Jen The Fredo":         date("2010-06-25"),
			"The Final Countdown":   date("2010-07-02"),
			"Something Happened":    date("2010-07-09"),
			"Italian For Beginners": date("2010-07-16"),
			"Bad Boys":              date("2010-07-23"),
			"Reynholm vs Reynholm":  date("2010-07-30"),
		}},
	} {
		seasonID := uuid.New().String()
		seasons = append(seasons, seasonData(seriesID, seasonID, season.title, season.first, season.last))
		for title, date := range season.episodes {
			episodes = append(episodes, episodeData(seriesID, seasonID, uuid.New().String(), title, date))
		}
	}
	return
}

func getDataForSiliconValley(seriesID string) (series *Series, seasons []*Seasons, episodes []*Episodes) {
	series = seriesData(
		seriesID, date("2014-04-06"), "Silicon Valley", ""+
			"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
			"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
		"Some comment here",
	)
	for _, season := range []struct {
		title    string
		first    sql.NullTime
		last     sql.NullTime
		episodes map[string]sql.NullTime
	}{
		{"Season 1", date("2014-04-06"), date("2014-06-01"), map[string]sql.NullTime{
			"Minimum Viable Product":        date("2014-04-06"),
			"The Cap Table":                 date("2014-04-13"),
			"Articles of Incorporation":     date("2014-04-20"),
			"Fiduciary Duties":              date("2014-04-27"),
			"Signaling Risk":                date("2014-05-04"),
			"Third Party Insourcing":        date("2014-05-11"),
			"Proof of Concept":              date("2014-05-18"),
			"Optimal Tip-to-Tip Efficiency": date("2014-06-01"),
		}},
		{"Season 2", date("2015-04-12"), date("2015-06-14"), map[string]sql.NullTime{
			"Sand Hill Shuffle":      date("2015-04-12"),
			"Runaway Devaluation":    date("2015-04-19"),
			"Bad Money":              date("2015-04-26"),
			"The Lady":               date("2015-05-03"),
			"Server Space":           date("2015-05-10"),
			"Homicide":               date("2015-05-17"),
			"Adult Content":          date("2015-05-24"),
			"White Hat/Black Hat":    date("2015-05-31"),
			"Binding Arbitration":    date("2015-06-07"),
			"Two Days of the Condor": date("2015-06-14"),
		}},
		{"Season 3", date("2016-04-24"), date("2016-06-26"), map[string]sql.NullTime{
			"Founder Friendly":               date("2016-04-24"),
			"Two in the Box":                 date("2016-05-01"),
			"Meinertzhagen's Haversack":      date("2016-05-08"),
			"Maleant Data Systems Solutions": date("2016-05-15"),
			"The Empty Chair":                date("2016-05-22"),
			"Bachmanity Insanity":            date("2016-05-29"),
			"To Build a Better Beta":         date("2016-06-05"),
			"Bachman's Earnings Over-Ride":   date("2016-06-12"),
			"Daily Active Users":             date("2016-06-19"),
			"The Uptick":                     date("2016-06-26"),
		}},
		{"Season 4", date("2017-04-23"), date("2017-06-25"), map[string]sql.NullTime{
			"Success Failure":       date("2017-04-23"),
			"Terms of Service":      date("2017-04-30"),
			"Intellectual Property": date("2017-05-07"),
			"Teambuilding Exercise": date("2017-05-14"),
			"The Blood Boy":         date("2017-05-21"),
			"Customer Service":      date("2017-05-28"),
			"The Patent Troll":      date("2017-06-04"),
			"The Keenan Vortex":     date("2017-06-11"),
			"Hooli-Con":             date("2017-06-18"),
			"Server Error":          date("2017-06-25"),
		}},
		{"Season 5", date("2018-03-25"), date("2018-05-13"), map[string]sql.NullTime{
			"Grow Fast or Die Slow":             date("2018-03-25"),
			"Reorientation":                     date("2018-04-01"),
			"Chief Operating Officer":           date("2018-04-08"),
			"Tech Evangelist":                   date("2018-04-15"),
			"Facial Recognition":                date("2018-04-22"),
			"Artificial Emotional Intelligence": date("2018-04-29"),
			"Initial Coin Offering":             date("2018-05-06"),
			"Fifty-One Percent":                 date("2018-05-13"),
		}},
	} {
		seasonID := uuid.New().String()
		seasons = append(seasons, seasonData(seriesID, seasonID, season.title, season.first, season.last))
		for title, date := range season.episodes {
			episodes = append(episodes, episodeData(seriesID, seasonID, uuid.New().String(), title, date))
		}
	}
	return
}

const dateISO8601 = "2006-01-02"

func date(date string) sql.NullTime {
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return sql.NullTime{Time: t, Valid: true}
}
