package main

import (
	mng "go_consumer/src/manager"
	"go_consumer/src/runner"
	"go_consumer/src/utils"
	"log"
	"strings"
)

func main()  {

	manager, err := mng.GetManager()
	utils.CheckFatal(err)

	_, err = manager.CreateDb()
	utils.CheckFatal(err)

	r, err := runner.GetRunnerFromConfig()
	utils.CheckFatal(err)

	c, err := utils.GetConfig("../conf/config.json")
	utils.CheckFatal(err)

	i := strings.LastIndex(c.MqttTopic, ".")
	if i == -1 {
		log.Fatal("Wrong topic format")
	}

	exchange, queue := c.GetMqttData()
	err = runner.Setup(r, exchange, "topic", queue, queue)
	utils.CheckFatal(err)
}