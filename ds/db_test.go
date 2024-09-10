package ds_test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/enorith/gormdb"
	"github.com/enorith/syncer/ds"
	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func loadEnv() {
	_ = godotenv.Load(".env")
}

type User struct {
	ID       int64
	Name     string
	Username string
}

func (User) AggTableScope() func(*gorm.DB) *gorm.DB {
	return func(d *gorm.DB) *gorm.DB {
		return d.Select("id")
	}
}

func (User) ListScope() func(*gorm.DB) *gorm.DB {
	return func(d *gorm.DB) *gorm.DB {
		return d
	}
}
func printJson(data any) {
	b, _ := json.MarshalIndent(data, "", "  ")

	fmt.Println(string(b))
}

func getDB() (*gorm.DB, error) {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  logger.Info, // Log level
			IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
			ParameterizedQueries:      true,        // Don't include params in the SQL log
			Colorful:                  false,       // Disable color
		},
	)

	return gorm.Open(mysql.Open(os.Getenv("DB_DSN")), &gorm.Config{
		Logger: newLogger,
	})
}

func TestDbSource(t *testing.T) {
	loadEnv()

	gormDB, e := getDB()

	if e != nil {
		t.Error(e)
	}
	db := ds.NewDB(gormDB, ds.NewDBConfig{
		Table: "users",
		PK:    "id",
		Model: User{},
	})

	db.Create(&User{
		Name:     "Nerio",
		Username: "NerioDev",
	})

	res, e := db.List(ds.ListOption{
		Orders:  []ds.ListOrder{{Field: "id", Order: "desc"}},
		Filters: []ds.ListFilter{},
		Page:    1,
		Limit:   20,
	})

	if e != nil {
		t.Error(e)
	}

	find, _ := db.Find(1)

	printJson(res)

	printJson(find)
}

func TestDSN(t *testing.T) {
	loadEnv()
	gormdb.DefaultManager.RegisterDefault(getDB)

	ds.RegisterDatasource("db", ds.DBRegister)

	ds.RegisterDBModel("users", User{})

	d, e := ds.Connect("db://default/users?pk=id")

	if e != nil {
		t.Error(e)
	}

	res, e := d.List(ds.ListOption{})
	if e != nil {
		t.Error(e)
	}
	printJson(res)
}
