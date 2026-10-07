package pubsub


import (
	"encoding/json"
	"encoding/gob"
	"context"
	"log"
	"fmt"
	"bytes"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int
type AckType int

const (
	Durable SimpleQueueType = iota
	Transient
	Ack AckType = iota
	NackRequeue
	NackDiscard
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var out bytes.Buffer        
	enc := gob.NewEncoder(&out)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType:  "application/gob",
		Body:         out.Bytes(),
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg) 
}

func SubscribeGob[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T)AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler,
		func(payload []byte) (T, error) {
			dec := gob.NewDecoder(bytes.NewBuffer(payload))
			var body T
			err := dec.Decode(&body)
			if err != nil {
				infof(fmt.Sprint("Got error when executing delivery. Skipping..\n"))
			}

			return body, err
		})
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
	
	table := amqp.Table{
    	"x-dead-letter-exchange": "peril_dlx",
	}
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, table)
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
    handler func(T)AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler,
		func(payload []byte) (T, error) {
			var body T
			err := json.Unmarshal(payload, &body)
			if err != nil {
				infof(fmt.Sprint("Got error when executing delivery. Skipping..\n"))
			}

			return body, err
		})
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T)AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		fmt.Printf("%v\n", err)
		return err
	}
	
	err = ch.Qos(10, 10, true)
	if err != nil {
		fmt.Printf("%v\n", err)
		return err
	}
	deliveryCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		fmt.Printf("%v\n", err)
		return err
	}

	go execute(deliveryCh, handler, unmarshaller)
	return nil
}

func execute[T any] (
	ch <-chan amqp.Delivery, 
	handler func(T) AckType, 
	unmarshaller func([]byte) (T, error)) {
	for d := range ch {
		body, err := unmarshaller(d.Body)
		if err != nil {
			fmt.Printf("%v\n", err)
			infof(fmt.Sprint("Got error when executing delivery. Skipping..\n"))
			continue
		}
		ackType := handler(body)
		switch ackType {
			case Ack:
				d.Ack(false)
			case NackRequeue:
				d.Nack(false, true)
			case NackDiscard:
				d.Nack(false, false)
			default:
				fmt.Printf("Unespected ackType %s", ackType) 
		}
	}
}
