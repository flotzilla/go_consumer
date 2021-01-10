package consumer

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"go_consumer/src/manager"
	"log"
)

type MessageConsumer struct {
	DB *manager.DB
}

type Payload struct {
	Message string `json:"message"`
	Name    string `json:"name"`
}

func (mh *MessageConsumer) Handle(message <-chan amqp.Delivery, status chan bool) {
	for d := range message {
		data := Payload{}
		err := json.Unmarshal(d.Body, &data)
		log.Println("Handling...")
		if err != nil {
			log.Println(err)
		} else {
			_, err = mh.DB.InsertMessage(data.Message, data.Name)
			if err != nil {
				log.Println(err)
			}
		}
	}

	log.Println("handle: deliveries channel closed")
	status <- true
}
