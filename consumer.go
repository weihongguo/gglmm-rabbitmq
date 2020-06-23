package rabbitmq

import (
	"errors"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// ConsumeFunc --
type ConsumeFunc func(exchangeName string, routingKey string, contentType string, body []byte) error

// Consumer --
type Consumer struct {
	connection     *amqp.Connection
	selfConnection bool
	channel        *amqp.Channel
	exchangeType   string
	queueName      string
}

// NewConsumerConfig --
func NewConsumerConfig(config ConfigRabbitmq) (*Consumer, error) {
	return NewConsumer(config.User, config.Password, config.Address)
}

// NewConsumer --
func NewConsumer(user string, password string, address string) (*Consumer, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s", user, password, address)
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, err
	}
	consumer := &Consumer{
		connection:     connection,
		selfConnection: true,
		channel:        channel,
	}
	return consumer, nil
}

// Close --
func (consumer *Consumer) Close() {
	consumer.channel.Close()
	if consumer.selfConnection {
		consumer.connection.Close()
	}
}

func (consumer *Consumer) consume(queueName string, consumerName string) (<-chan amqp.Delivery, error) {
	return consumer.channel.Consume(queueName, consumerName, false, false, false, false, nil)
}

// TypeDirect --
func (consumer *Consumer) TypeDirect(queueName string) error {
	consumer.exchangeType = amqp.ExchangeDirect
	consumer.queueName = queueName
	log.Printf("%s : %s", consumer.exchangeType, queueName)
	_, err := queueDeclare(consumer.channel, queueName)
	if err != nil {
		consumer.Close()
		return err
	}
	return nil
}

// TypeFanout --
func (consumer *Consumer) TypeFanout(queueName string, exchangeName string) error {
	consumer.exchangeType = amqp.ExchangeFanout
	consumer.queueName = queueName
	log.Printf("%s : %s on %s", consumer.exchangeType, queueName, exchangeName)
	err := exchangeDeclare(consumer.channel, exchangeName, consumer.exchangeType)
	if err != nil {
		consumer.Close()
		return err
	}
	queue, err := queueDeclare(consumer.channel, queueName)
	if err != nil {
		consumer.Close()
		return err
	}
	err = queueBind(consumer.channel, queue.Name, "", exchangeName)
	if err != nil {
		consumer.Close()
		return err
	}
	return nil
}

// TypeTopic --
func (consumer *Consumer) TypeTopic(queueName string, routingKey string, exchangeName string) error {
	consumer.exchangeType = amqp.ExchangeTopic
	consumer.queueName = queueName
	log.Printf("%s : %s on %s by %s", consumer.exchangeType, queueName, routingKey, exchangeName)
	err := exchangeDeclare(consumer.channel, exchangeName, consumer.exchangeType)
	if err != nil {
		consumer.Close()
		return err
	}
	queue, err := queueDeclare(consumer.channel, queueName)
	if err != nil {
		consumer.Close()
		return err
	}
	err = queueBind(consumer.channel, queue.Name, routingKey, exchangeName)
	if err != nil {
		consumer.Close()
		return err
	}
	return nil
}

// Subscribe --
func (consumer *Consumer) Subscribe(consumeFunc ConsumeFunc) (chan<- interface{}, error) {
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
