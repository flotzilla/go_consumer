package manager

import (
	"database/sql"
	"errors"
	_ "go_consumer/src"
	"go_consumer/src/utils"
	"log"
	"os"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	DSN  string
	db   *sql.DB
	lock sync.Mutex
}

var (
	instance *DB
)

func GetManager() (*DB, error) {
	if instance != nil {
		return instance, nil
	}

	instance = &DB{}

	log.Println("Creating new connector")
	conf, err := utils.GetConfig("../conf/config.json")

	if err != nil {
		log.Println(err)
		return nil, err
	}

	instance.DSN = conf.DSN

	return instance, nil
}

func (db *DB) CreateDb() (bool, error) {
	db.lock.Lock()

	defer db.lock.Unlock()

	if _, err := os.Stat(db.DSN); os.IsNotExist(err) {
		file, err := os.Create(db.DSN)
		if err != nil {
			log.Println(err)
			return false, err
		}
		defer file.Close()

		log.Println("db file created, creating tables")
		_, err = db.runCommand("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, message TEXT, name TEXT)")

		if err != nil {
			log.Println(err)
		}

		log.Print("Db successfully created")

		return true, nil
	}

	return false, errors.New(db.DSN + " file already exists")

}

func (db *DB) InsertMessage(message string, name string) (sql.Result, error) {
	return db.runCommandWithParams("INSERT INTO messages (message, name) VALUES ($1, $2);", message, name)
}

func (db *DB) runCommand(sqlString string) (sql.Result, error) {
	defer func() {
		_ = Close(db)
	}()

	return runSQL(db, sqlString)
}

func runSQL(manager *DB, sqlString string) (sql.Result, error) {

	Connect(manager)
	st, err := manager.db.Prepare(sqlString)

	if err != nil {
		return nil, err
	}

	return st.Exec()
}

func runSQLWithParams(manager *DB, sqlString string, args ...string) (sql.Result, error) {
	Connect(manager)
	st, err := manager.db.Prepare(sqlString)
	defer st.Close()

	if err != nil {
		return nil, err
	}

	var ar []interface{}
	for _, v := range args{
		ar = append(ar, v)
	}

	return st.Exec(ar...)
}

func (db *DB) runCommandWithParams(sqlString string, args ...string) (sql.Result, error) {
	defer func() {
		_ = Close(db)
	}()

	return runSQLWithParams(db, sqlString, args...)
}

func Connect(manager *DB) *sql.DB {
	db, err := sql.Open("sqlite3", manager.DSN)
	if err != nil {
		if db != nil {
			_ = Close(manager)
		}

		log.Fatal(err)
	}

	manager.db = db

	return db
}

func Close(manager *DB) error {
	if manager.db != nil {
		err := manager.db.Close()
		if err != nil {
			log.Println(err)
		}

		return err
	}

	return nil
}
