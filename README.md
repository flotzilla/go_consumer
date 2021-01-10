Golang consumer demo
=====

### Build and run
* run `go build cmd/message_consumer.go` and `go build cmd/init.go`
* run `docker/docker-composer up -d`
* run `./cmd/init` to initialize db and create exchanges
* run `./cmd/go_consumer` to run app, `ctr+c` to stop

### Sending json payload
open [web-manager](http://127.0.0.1:15672/#/exchanges/%2F/amqp.messaging)
