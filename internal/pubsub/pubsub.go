package pubsub


import (
	"encoding/json"
	"context"
	"log"
	"fmt"
	"bytes"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

var (
	buf    bytes.Buffer
	logger = log.New(&buf, "INFO: ", log.Lshortfile)

	infof = func(info string) {
		logger.Output(2, info)
	}
)


func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	out, err := json.Marshal(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         out,
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg) 
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, *amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err	
	}

	durable := false
	if queueType == Durable{
		durable = true
	}

	autoDelete := false
	exclusive := false
	if queueType == Transient{
		autoDelete = true
		exclusive = true
	}

	// name string, durable, autoDelete, exclusive, noWait bool, args Table
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return nil, nil, err	
	}
	
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, nil, err	
	}
	return ch, &queue, nil
}



func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	
	deliveryCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go execute(deliveryCh, handler)
	return nil
}


func execute[T any] (ch <-chan amqp.Delivery, handler func(T)) {
	for d := range ch {
		var body T
		err := json.Unmarshal(d.Body, &body)
		if err != nil {
			infof(fmt.Sprint("Got error when executing delivery. Skipping..\n"))
			continue
		}
		fmt.Printf("executing %s", string(d.Body))
		handler(body)
		d.Ack(false)
		if err != nil {
			infof(fmt.Sprint("Got error when ack delivery. Skipping..\n"))
			continue
		}
	}
}
