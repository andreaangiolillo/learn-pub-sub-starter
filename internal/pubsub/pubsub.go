package pubsub


import (
	"encoding/json"
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
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
