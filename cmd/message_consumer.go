package main

import (
	"flag"
	"go_consumer/src/consumer"
	"go_consumer/src/manager"
	"go_consumer/src/runner"
	"go_consumer/src/utils"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	lifetime = flag.Duration("lifetime", 0, "lifetime before shutdown")
	service = runner.Runner{}
)

func main() {
	m, err := manager.GetManager()
	utils.CheckFatal(err)

	service, err := runner.GetRunnerFromConfig()
	utils.CheckFatal(err)

	err = service.Start()
	utils.CheckFatal(err)

	mc := &consumer.MessageConsumer{
		DB: m,
	}
	c, e := service.ConsumeFromConfig(mc)
	utils.CheckFatal(e)

	closeNotifier := make(chan os.Signal, 1)
	signal.Notify(closeNotifier, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	if *lifetime > 0 {
		log.Printf("running for %s", *lifetime)
		time.Sleep(*lifetime)
	} else {
		log.Println("running forever")
		defer func() {
			c.Stop()
			utils.CheckFatal(service.Stop())
			log.Println("Bye")
		}()
		<-closeNotifier
	}
}

