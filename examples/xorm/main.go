package main

import (
	"context"
	"log"
	"os"
	"time"

	"xorm.io/builder"
	"xorm.io/xorm"
	xormLog "xorm.io/xorm/log"

	_ "github.com/ydb-platform/ydb-go-sdk/v3"

	_ "github.com/go-sql-driver/mysql"

	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
)

var envNotFoundMessage = `DSN environment variable not defined

Use any of these:
POSTGRES_CONNECTION_STRING
SQLITE_CONNECTION_STRING
YDB_CONNECTION_STRING`

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		db  *xorm.Engine
		err error
	)
	if dsn, exists := os.LookupEnv("POSTGRES_CONNECTION_STRING"); exists {
		db, err = xorm.NewEngine("postgres", dsn)
	} else if dsn, exists = os.LookupEnv("SQLITE_CONNECTION_STRING"); exists {
		db, err = xorm.NewEngine("mysql", dsn)
	} else if dsn, exists = os.LookupEnv("YDB_CONNECTION_STRING"); exists {
		db, err = xorm.NewEngine("ydb", dsn)
	} else {
		panic(envNotFoundMessage)
	}

	if err != nil {
		panic(err)
	}

	db.ShowSQL(false)
	db.SetLogLevel(xormLog.LOG_DEBUG)

	// prepare scheme 
	if err = prepareScheme(ctx, db); err != nil {
		panic(err)
	}

	// fill data
	if err = fillData(ctx, db); err != nil {
		panic(err)
	}

	// read all data
	if err = readAll(ctx, db); err != nil {
		panic(err)
	}

	// find by condition
	if err = findEpisodesByTitle(ctx, db, "Bad"); err != nil {
		panic(err)
	}
}

func prepareScheme(ctx context.Context, db *xorm.Engine) error {
	if err := db.DropTables(&Series{}, &Seasons{}, &Episodes{}); err != nil {
		return err
	}
	if err := db.CreateTables(&Series{}, &Seasons{}, &Episodes{}); err != nil {
		return err
	}
	return nil
}

func fillData(ctx context.Context, db *xorm.Engine) error {
	series, seasons, episodes := getData()

	session := db.NewSession().Context(ctx)
	defer session.Close()

	if _, err := session.Insert(&series, &seasons, &episodes); err != nil {
		return err
	}
	return nil
}

func readAll(ctx context.Context, db *xorm.Engine) error {
	session := db.NewSession().Context(ctx)
	defer session.Close()

	var series []*Series
	if err := session.Find(&series); err != nil {
		return err
	}

	for _, s := range series {
		log.Printf(
			"  > [%s]     %s (%s)\n",
			s.ID, s.Title, s.ReleaseDate.Format("2006"),
		)

		var seasons []*Seasons
		if err := session.Where(builder.Eq{
			"series_id": s.ID,
		}).Find(&seasons); err != nil {
			return err
		}

		for _, ss := range seasons {
			log.Printf(
				"    > [%s]   %s\n",
				ss.ID, ss.Title,
			)

			var episodes []*Episodes
			if err := session.Where(builder.Eq{
				"season_id": ss.ID,
			}).Find(&episodes); err != nil {
				return err
			}
			for _, e := range episodes {
				log.Printf(
					"      > [%s] [%s] %s\n",
					e.ID, e.AirDate.Format(dateISO8601), e.Title,
				)
			}
		}
	}

	return nil
}

func findEpisodesByTitle(ctx context.Context, db *xorm.Engine, fragment string) error {
	session := db.NewSession().Context(ctx)
	defer session.Close()

	var episodes []*Episodes
	if err := session.Where(builder.Like{
		"title",
		"%" + fragment + "%",
	}).Find(&episodes); err != nil {
		return err
	}

	log.Println("all episodes with title with word 'bad':")
	for _, e := range episodes {
		ss := Seasons{
			ID: e.SeasonID,
		}
		if _, err := session.Get(&ss); err != nil {
			return err
		}

		s := Series{
			ID: ss.SeriesID,
		}
		if _, err := session.Get(&s); err != nil {
			return err
		}
		log.Printf(
			"  > [%s]     %s (%s)\n",
			s.ID, s.Title, s.ReleaseDate.Format("2006"),
		)
		log.Printf(
			"    > [%s]   %s\n",
			ss.ID, ss.Title,
		)
		log.Printf(
			"      > [%s] [%s] %s\n",
			e.ID, e.AirDate.Format(dateISO8601), e.Title,
		)
	}
	return nil
}
