package runner

import (
	"github.com/streadway/amqp"
	"go_consumer/src/utils"
	"log"
)

type Runner struct {
	DSN        string
	connection *amqp.Connection
}

type Consumer struct {
	Queue    string
	Consumer string
	Channel  *amqp.Channel
	done     chan bool
}

type ConsumerInterface interface {
	Stop() error
}


type HandlerInterface interface {
	Handle(message <-chan amqp.Delivery, status chan bool)
}

var runner *Runner

func GetRunnerFromConfig() (*Runner, error) {
	if runner != nil {
		return runner, nil
	}

	runner = &Runner{}

	conf, err := utils.GetConfig("../conf/config.json")

	if err != nil {
		log.Println(err)
	}

	runner.DSN = conf.MqttDSN
	return runner, nil
}

func Setup(r *Runner, exchange string, exchangeType string, queue string, queueKey string) error {
	log.Println("Creating amqp exchange and queue")
	err := r.Start()
	if err != nil {
		log.Println(err)
		return err
	}

	channel, err := r.connection.Channel()
	if err != nil {
		log.Println(err)
		return err
	}

	err = channel.ExchangeDeclare(exchange, exchangeType, true, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("Exchange created")

	_, err = channel.QueueDeclare(queue, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return err
	}

	err = channel.QueueBind(queue, queueKey, exchange, false, nil)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("Queue created")

	err = channel.Close()

	if err != nil {
		log.Println(err)
		return err
	}

	return r.Stop()
}

func (r *Runner) Start() error {
	connection, err := amqp.Dial(r.DSN)

	if err != nil {
		return err
	}

	r.connection = connection
	return nil
}

func (r *Runner) Stop() error {

	log.Println("closing connection")

	if r == nil {
		return nil
	}

	err := r.connection.Close()
	if err != nil {
		log.Println(err)
	}

	return err
}

func (r *Runner) ConsumeFromConfig (handler HandlerInterface)  (*Consumer, error){
	conf, err := utils.GetConfig("../conf/config.json")

	if err != nil {
		log.Print(err)
		return nil, err
	}

	queue, exchange := conf.GetMqttData()
	return r.Consume(exchange, queue, handler)
}


func (r *Runner) Consume(queue string, consumer string, handler HandlerInterface) (*Consumer, error) {
	cs := &Consumer{
		Queue:    queue,
		Consumer: consumer,
		done:     make(chan bool),
	}

	c, err := r.connection.Channel()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	cs.Channel = c
	msg, err := c.Consume(queue, consumer, true, false, false, false, nil)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	go handler.Handle(msg, cs.done)

	return cs, nil
}

func (c *Consumer) Stop() bool {
	log.Println("closing channel")
	if err := c.Channel.Cancel(c.Consumer, true); err != nil {
		log.Println(err)
		return false
	}

	return <-c.done
}
