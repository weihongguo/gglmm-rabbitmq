package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

// Producer --
type Producer struct {
	connection     *amqp.Connection
	selfConnection bool
	channel        *amqp.Channel
	exchangeType   string
	exchangeName   string
}

// NewProducerConfig --
func NewProducerConfig(config ConfigRabbitmq) (*Producer, error) {
	return NewProducer(config.User, config.Password, config.Address)
}

// NewProducer --
func NewProducer(user string, password string, address string) (*Producer, error) {
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
	producer := &Producer{
		connection:     connection,
		selfConnection: true,
		channel:        channel,
	}
	return producer, nil
}

// Close --
func (producer *Producer) Close() {
	producer.channel.Close()
	if producer.selfConnection {
		producer.connection.Close()
	}
}

func (producer *Producer) publish(exchangeName string, routingKey string, contentType string, content []byte) error {
	return producer.channel.Publish(
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

// TypeDirect --
func (producer *Producer) TypeDirect(queueName string) error {
	producer.exchangeType = amqp.ExchangeDirect
	producer.exchangeName = ""
	_, err := queueDeclare(producer.channel, queueName)
	if err != nil {
		producer.Close()
		return err
	}
	return nil
}

// TypeFanout --
func (producer *Producer) TypeFanout(exchangeName string) error {
	producer.exchangeType = amqp.ExchangeFanout
	producer.exchangeName = exchangeName
	err := exchangeDeclare(producer.channel, exchangeName, producer.exchangeType)
	if err != nil {
		producer.Close()
		return err
	}
	return nil
}

// TypeTopic --
func (producer *Producer) TypeTopic(exchangeName string) error {
	producer.exchangeType = amqp.ExchangeTopic
	producer.exchangeName = exchangeName
	err := exchangeDeclare(producer.channel, exchangeName, producer.exchangeType)
	if err != nil {
		producer.Close()
		return err
	}
	return nil
}

// Publish --
func (producer *Producer) Publish(routingKey string, message interface{}) error {
	if producer.exchangeType == "" {
		return errors.New("excange type error")
	}
	if producer.exchangeType == amqp.ExchangeFanout {
		routingKey = ""
	}
	content, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return producer.publish(
		producer.exchangeName,
		routingKey,
		"application/json",
		content,
	)
}
