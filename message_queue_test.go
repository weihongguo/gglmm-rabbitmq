package rabbitmq

import (
	"fmt"
	"log"
	"testing"
	"time"
)

type Message struct {
	Content string `json:"content"`
}

func TestDirect(t *testing.T) {
	config := ConfigRabbitmq{
		User:     "guoguo",
		Password: "123456",
		Address:  ":5672",
	}
	queueName := "gglmm-test-mq"

	var stopChan chan<- interface{}

	go func() {
		consumer := NewConsumerConfig(config)
		consumer.TypeDirect(queueName)
		var err error
		stopChan, err = consumer.Consume(func(exchangeName string, routingKey string, contentType string, body []byte) error {
			log.Printf("%s : %s %s %s %s\n", queueName, contentType, exchangeName, routingKey, string(body))
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(1 * time.Second)

	publisher := NewPublisherConfig(config)
	publisher.TypeDirect(queueName)

	var i = 0
	for i = 0; i < 2; i++ {
		timer := time.NewTimer(time.Second * 1)
		select {
		case <-timer.C:
			err := publisher.Publish(queueName, Message{
				Content: fmt.Sprintf("order: %d", i),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	time.Sleep(1 * time.Second)

	stopChan <- 1
}

func TestFanout(t *testing.T) {
	config := ConfigRabbitmq{
		User:     "guoguo",
		Password: "123456",
		Address:  ":5672",
	}
	routingKeyToQueueName := map[string]string{
		"gglmm-test-routing1": "gglmm-test-fanout-mq1",
		"gglmm-test-routing2": "gglmm-test-fanout-mq2",
	}
	exchangeName := "gglmm-test-exchange"

	queueNameTostopChan := make(map[string]chan<- interface{})

	for routingKey, queueName := range routingKeyToQueueName {
		go func(queueName string, routingKey string) {
			consumer := NewConsumerConfig(config)
			consumer.TypeFanout(queueName, routingKey, exchangeName)
			stopChan, err := consumer.Consume(func(exchangeName string, routingKey string, contentType string, body []byte) error {
				log.Printf("%s : %s %s %s %s\n", queueName, contentType, exchangeName, routingKey, string(body))
				return nil
			})
			if err != nil {
				log.Fatal(err)
			}
			queueNameTostopChan[queueName] = stopChan
		}(queueName, routingKey)
	}

	time.Sleep(1 * time.Second)

	publisher := NewPublisherConfig(config)
	publisher.TypeFanout(exchangeName)

	var i = 0
	for i = 0; i < 2; i++ {
		timer := time.NewTimer(time.Second * 1)
		select {
		case <-timer.C:
			err := publisher.Publish("", Message{
				Content: fmt.Sprintf("order: %d", i),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	time.Sleep(1 * time.Second)

	for _, stopChan := range queueNameTostopChan {
		stopChan <- 1
	}
}

func TestTopic(t *testing.T) {
	config := ConfigRabbitmq{
		User:     "guoguo",
		Password: "123456",
		Address:  ":5672",
	}
	routingKeyToQueueName := map[string]string{
		"gglmm.test.update.*": "gglmm-test-topic-update",
		"gglmm.test.create.*": "gglmm-test-topic-create",
	}
	exchangeName := "gglmm-test-topic"

	queueNameTostopChan := make(map[string]chan<- interface{})

	for routingKey, queueName := range routingKeyToQueueName {
		go func(queueName string, routingKey string) {
			consumer := NewConsumerConfig(config)
			consumer.TypeTopic(queueName, routingKey, exchangeName)
			stopChan, err := consumer.Consume(func(exchangeName string, routingKey string, contentType string, body []byte) error {
				log.Printf("%s : %s %s %s %s\n", queueName, contentType, exchangeName, routingKey, string(body))
				return nil
			})
			if err != nil {
				log.Fatal(err)
			}
			queueNameTostopChan[queueName] = stopChan
		}(queueName, routingKey)
	}

	time.Sleep(1 * time.Second)

	publisher := NewPublisherConfig(config)
	publisher.TypeTopic(exchangeName)

	var i = 0
	for i = 0; i < 2; i++ {
		timer := time.NewTimer(time.Second * 1)
		select {
		case <-timer.C:
			err := publisher.Publish("gglmm.test.update.1", Message{
				Content: fmt.Sprintf("order: %d", i),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	time.Sleep(1 * time.Second)

	for _, stopChan := range queueNameTostopChan {
		stopChan <- 1
	}
}
