package rabbitmq

import (
	"fmt"
	"log"
	"testing"
	"time"
)

var mq *RabbitMQ

func TestMain(m *testing.M) {
	config := ConfigRabbitmq{
		User:     "guoguo",
		Password: "123456",
		Address:  ":5672",
	}
	var err error
	mq, err = NewRabbitMQConfig(config)
	if err != nil {
		log.Fatal(err)
		return
	}
	m.Run()
}

type Message struct {
	Content string `json:"content"`
}

func TestDirect(t *testing.T) {

	queueName := "gglmm-test-mq"

	var stopChan chan<- interface{}

	go func() {
		consumer, err := mq.Consumer()
		if err != nil {
			log.Fatal(err)
		}
		consumer.TypeDirect(queueName)
		stopChan, err = consumer.Subscribe(func(exchangeName string, routingKey string, contentType string, body []byte) error {
			log.Printf("%s : %s %s %s %s\n", queueName, contentType, exchangeName, routingKey, string(body))
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(1 * time.Second)

	producer, err := mq.Producer()
	if err != nil {
		t.Fatal(err)
	}
	producer.TypeDirect(queueName)

	var i = 0
	for i = 0; i < 2; i++ {
		timer := time.NewTimer(time.Second * 1)
		select {
		case <-timer.C:
			err := producer.Publish(queueName, Message{
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
	routingKeyToQueueName := map[string]string{
		"gglmm-test-routing1": "gglmm-test-fanout-mq1",
		"gglmm-test-routing2": "gglmm-test-fanout-mq2",
	}
	exchangeName := "gglmm-test-exchange"

	queueNameTostopChan := make(map[string]chan<- interface{})

	for routingKey, queueName := range routingKeyToQueueName {
		go func(queueName string, routingKey string) {
			consumer, err := mq.Consumer()
			if err != nil {
				log.Fatal(err)
			}
			consumer.TypeFanout(queueName, exchangeName)
			stopChan, err := consumer.Subscribe(func(exchangeName string, routingKey string, contentType string, body []byte) error {
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

	producer, err := mq.Producer()
	if err != nil {
		log.Fatal(err)
	}
	producer.TypeFanout(exchangeName)

	var i = 0
	for i = 0; i < 2; i++ {
		timer := time.NewTimer(time.Second * 1)
		select {
		case <-timer.C:
			err := producer.Publish("", Message{
				Content: fmt.Sprintf("order: %d", i),
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	time.Sleep(1 * time.Second)

	for _, stopChan := range queueNameTostopChan {
		stopChan <- 1
	}
}

func TestTopic(t *testing.T) {
	routingKeyToQueueName := map[string]string{
		"gglmm.test.update.*": "gglmm-test-topic-update",
		"gglmm.test.create.*": "gglmm-test-topic-create",
	}
	exchangeName := "gglmm-test-topic"

	queueNameTostopChan := make(map[string]chan<- interface{})

	for routingKey, queueName := range routingKeyToQueueName {
		go func(queueName string, routingKey string) {
			consumer, err := mq.Consumer()
			if err != nil {
				log.Fatal(err)
			}
			consumer.TypeTopic(queueName, routingKey, exchangeName)
			stopChan, err := consumer.Subscribe(func(exchangeName string, routingKey string, contentType string, body []byte) error {
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

	producer, err := mq.Producer()
	if err != nil {
		log.Fatal(err)
	}
	producer.TypeTopic(exchangeName)

	var i = 0
	for i = 0; i < 2; i++ {
		timer := time.NewTimer(time.Second * 1)
		select {
		case <-timer.C:
			err := producer.Publish("gglmm.test.update.1", Message{
				Content: fmt.Sprintf("order: %d", i),
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	time.Sleep(1 * time.Second)

	for _, stopChan := range queueNameTostopChan {
		stopChan <- 1
	}
}
