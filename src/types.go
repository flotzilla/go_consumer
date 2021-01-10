package src

import (
	"database/sql"
	"log"
	"strings"
)

type DbManager interface {
	CreateDb() (bool, error)
	InsertMessage(message string, name string) (sql.Result, error)
}

type Config struct {
	DSN       string
	MqttDSN   string
	MqttTopic string
}

func (c *Config) GetMqttData() (string, string) {
	i := strings.LastIndex(c.MqttTopic, ".")
	if i == -1 {
		log.Fatal("Wrong topic format")
	}
	return c.MqttTopic[0:i], c.MqttTopic[i+1:]
}