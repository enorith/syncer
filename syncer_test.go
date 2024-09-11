package syncer_test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/enorith/gormdb"
	"github.com/enorith/syncer"
	"github.com/enorith/syncer/ds"
	"github.com/joho/godotenv"
	jsoniter "github.com/json-iterator/go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func loadEnv() {
	_ = godotenv.Load(".env")
}

func printJson(data any) {
	b, _ := json.MarshalIndent(data, "", "  ")

	fmt.Println(string(b))
}

func getDB(dsn string) gormdb.Register {
	return func() (*gorm.DB, error) {
		newLogger := logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             time.Second, // Slow SQL threshold
				LogLevel:                  logger.Info, // Log level
				IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
				ParameterizedQueries:      false,       // Don't include params in the SQL log
				Colorful:                  false,       // Disable color
			},
		)

		return gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: newLogger,
		})
	}
}

func TestSync(t *testing.T) {
	loadEnv()
	gormdb.DefaultManager.Register("remote", getDB(os.Getenv("REMOTE_DSN")))
	ds.RegisterDatasource("db", ds.DBRegister)

	local, e := getDB(os.Getenv("DB_DSN"))()

	if e != nil {
		t.Fatal(e)
	}

	syncer.RegisterTarget("default", syncer.NewDBTarget(local))

	content, e := os.ReadFile("syncer.example.json")

	if e != nil {
		t.Fatal(e)
	}

	var confs []syncer.SyncerTask
	e = jsoniter.Unmarshal(content, &confs)

	if e != nil {
		t.Fatal(e)
	}

	sy := syncer.NewSyncer()

	sy.AddTask(confs...)

	syncCount, e := sy.DoSync("sync_roles")

	t.Log(syncCount, e)
}

func TestSource(t *testing.T) {
	loadEnv()
	gormdb.DefaultManager.Register("remote", getDB(os.Getenv("REMOTE_DSN")))
	ds.RegisterDatasource("db", ds.DBRegister)

	source, er := ds.Connect("db://remote/users")

	if er != nil {
		t.Fatal(er)
	}

	res, e := source.List(ds.ListOption{
		Limit: 20,
	})

	if e != nil {
		t.Fatal(e)
	}

	printJson(res)
}
