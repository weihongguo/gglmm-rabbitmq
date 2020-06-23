package rabbitmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

// RabbitMQ --
type RabbitMQ struct {
	connection *amqp.Connection
}

// NewRabbitMQConfig --
func NewRabbitMQConfig(config ConfigRabbitmq) (*RabbitMQ, error) {
	return NewRabbitMQ(config.User, config.Password, config.Address)
}

// NewRabbitMQ --
func NewRabbitMQ(user string, password string, address string) (*RabbitMQ, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s", user, password, address)
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	mq := &RabbitMQ{
		connection: connection,
	}
	return mq, nil
}

// Close --
func (mq *RabbitMQ) Close() {
	mq.connection.Close()
}

// Producer --
func (mq *RabbitMQ) Producer() (*Producer, error) {
	channel, err := mq.connection.Channel()
	if err != nil {
		return nil, err
	}
	producer := &Producer{
		connection:     mq.connection,
		selfConnection: false,
		channel:        channel,
	}
	return producer, nil
}

// Consumer --
func (mq *RabbitMQ) Consumer() (*Consumer, error) {
	channel, err := mq.connection.Channel()
	if err != nil {
		return nil, err
	}
	consumer := &Consumer{
		connection:     mq.connection,
		selfConnection: false,
		channel:        channel,
	}
	return consumer, nil
}

func exchangeDeclare(channel *amqp.Channel, exchangeName string, exchangeType string) error {
	return channel.ExchangeDeclare(exchangeName, exchangeType, true, false, false, true, nil)
}

func queueDeclare(channel *amqp.Channel, queueName string) (amqp.Queue, error) {
	return channel.QueueDeclare(queueName, true, false, false, true, nil)
}

func queueBind(channel *amqp.Channel, queueName string, routingKey string, exchangeName string) error {
	return channel.QueueBind(queueName, routingKey, exchangeName, true, nil)
}
