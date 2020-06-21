package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// RabbitMQ --
type RabbitMQ struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// NewRabbitMQConfig --
func NewRabbitMQConfig(config ConfigRabbitmq) *RabbitMQ {
	return NewRabbitMQ(config.User, config.Password, config.Address)
}

// NewRabbitMQ --
func NewRabbitMQ(user string, password string, address string) *RabbitMQ {
	url := fmt.Sprintf("amqp://%s:%s@%s", user, password, address)
	connection, err := amqp.Dial(url)
	if err != nil {
		log.Fatal(err)
	}
	channel, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}
	return &RabbitMQ{
		connection: connection,
		channel:    channel,
	}
}

// Close --
func (mq *RabbitMQ) Close() {
	log.Println("RabbitMQ Close()")
	mq.channel.Close()
	mq.connection.Close()
}

func (mq *RabbitMQ) exchangeDeclare(exchangeName string, exchangeType string) error {
	return mq.channel.ExchangeDeclare(exchangeName, exchangeType, true, false, false, true, nil)
}

func (mq *RabbitMQ) queueDeclare(queueName string) (amqp.Queue, error) {
	return mq.channel.QueueDeclare(queueName, true, false, false, true, nil)
}

func (mq *RabbitMQ) queueBind(queueName string, routingKey string, exchangeName string) error {
	return mq.channel.QueueBind(queueName, routingKey, exchangeName, true, nil)
}

func (mq *RabbitMQ) publish(exchangeName string, routingKey string, contentType string, content []byte) error {
	return mq.channel.Publish(
		exchangeName,
		routingKey,
		true,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        content,
		},
	)
}

func (mq *RabbitMQ) consume(queueName string, consumerName string) (<-chan amqp.Delivery, error) {
	return mq.channel.Consume(queueName, consumerName, false, false, false, false, nil)
}

// Publisher --
type Publisher struct {
	*RabbitMQ
	exchangeType string
	exchangeName string
}

// NewPublisherConfig --
func NewPublisherConfig(config ConfigRabbitmq) *Publisher {
	return &Publisher{
		RabbitMQ: NewRabbitMQConfig(config),
	}
}

// NewPublisher --
func NewPublisher(user string, password string, address string) *Publisher {
	return &Publisher{
		RabbitMQ: NewRabbitMQ(user, password, address),
	}
}

// TypeDirect --
func (publisher *Publisher) TypeDirect(queueName string) {
	publisher.exchangeType = amqp.ExchangeDirect
	publisher.exchangeName = ""
	_, err := publisher.queueDeclare(queueName)
	if err != nil {
		publisher.Close()
		log.Println(err)
	}
}

// TypeFanout --
func (publisher *Publisher) TypeFanout(exchangeName string) {
	publisher.exchangeType = amqp.ExchangeFanout
	publisher.exchangeName = exchangeName
	err := publisher.exchangeDeclare(exchangeName, publisher.exchangeType)
	if err != nil {
		publisher.Close()
		log.Fatal(err)
	}

}

// TypeTopic --
func (publisher *Publisher) TypeTopic(exchangeName string) {
	publisher.exchangeType = amqp.ExchangeTopic
	publisher.exchangeName = exchangeName
	err := publisher.exchangeDeclare(exchangeName, publisher.exchangeType)
	if err != nil {
		publisher.Close()
		log.Fatal(err)
	}
}

// Publish --
func (publisher *Publisher) Publish(routingKey string, message interface{}) error {
	logRoutingKey := routingKey
	if routingKey == "" {
		logRoutingKey = "*"
	}
	logExchangeName := publisher.exchangeName
	if publisher.exchangeName == "" {
		logExchangeName = "*"
	}
	log.Printf("%s : %s by %s", publisher.exchangeType, logRoutingKey, logExchangeName)
	if publisher.exchangeType == "" {
		return errors.New("excange type error")
	}
	content, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return publisher.publish(
		publisher.exchangeName,
		routingKey,
		"application/json",
		content,
	)
}

// ConsumeFunc --
type ConsumeFunc func(exchangeName string, routingKey string, contentType string, body []byte) error

// Consumer --
type Consumer struct {
	*RabbitMQ
	exchangeType string
	queueName    string
}

// NewConsumerConfig --
func NewConsumerConfig(config ConfigRabbitmq) *Consumer {
	return &Consumer{
		RabbitMQ: NewRabbitMQConfig(config),
	}
}

// NewConsumer --
func NewConsumer(user string, password string, address string) *Consumer {
	return &Consumer{
		RabbitMQ: NewRabbitMQ(user, password, address),
	}
}

// TypeDirect --
func (consumer *Consumer) TypeDirect(queueName string) {
	consumer.exchangeType = amqp.ExchangeDirect
	consumer.queueName = queueName
	_, err := consumer.queueDeclare(queueName)
	if err != nil {
		consumer.Close()
		log.Println(err)
	}

}

// TypeFanout --
func (consumer *Consumer) TypeFanout(queueName string, routingKey string, exchangeName string) {
	consumer.exchangeType = amqp.ExchangeFanout
	consumer.queueName = queueName
	log.Printf("%s : %s on %s by %s", consumer.exchangeType, queueName, routingKey, exchangeName)
	err := consumer.exchangeDeclare(exchangeName, consumer.exchangeType)
	if err != nil {
		consumer.Close()
		log.Fatal(err)
	}
	queue, err := consumer.queueDeclare(queueName)
	if err != nil {
		consumer.Close()
		log.Fatal(err)
	}
	err = consumer.queueBind(queue.Name, routingKey, exchangeName)
	if err != nil {
		consumer.Close()
		log.Fatal(err)
	}
}

// TypeTopic --
func (consumer *Consumer) TypeTopic(queueName string, routingKey string, exchangeName string) {
	consumer.exchangeType = amqp.ExchangeTopic
	consumer.queueName = queueName
	log.Printf("%s : %s on %s by %s", consumer.exchangeType, queueName, routingKey, exchangeName)
	err := consumer.exchangeDeclare(exchangeName, consumer.exchangeType)
	if err != nil {
		consumer.Close()
		log.Fatal(err)
	}
	queue, err := consumer.queueDeclare(queueName)
	if err != nil {
		consumer.Close()
		log.Fatal(err)
	}
	err = consumer.queueBind(queue.Name, routingKey, exchangeName)
	if err != nil {
		consumer.Close()
		log.Fatal(err)
	}
}

// Consume --
func (consumer *Consumer) Consume(consumeFunc ConsumeFunc) (chan<- interface{}, error) {
	if consumer.exchangeType == "" {
		return nil, errors.New("excange type error")
	}

	delivery, err := consumer.consume(consumer.queueName, "")
	if err != nil {
		return nil, err
	}

	stop := false
	go func() {
		defer consumer.Close()
		for msg := range delivery {
			msg.Ack(false)
			consumeFunc(msg.Exchange, msg.RoutingKey, msg.ContentType, msg.Body)
			if stop {
				return
			}
		}
	}()

	stopChan := make(chan interface{})
	go func() {
		defer close(stopChan)
		<-stopChan
		log.Printf("message queue [%s] will stop!\n", consumer.queueName)
		stop = true
	}()

	return stopChan, nil
}
