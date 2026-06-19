package kafka

import (
	"encoding/json"
	"fmt"
	"log"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/database"
	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/entity"
)

type Consumer struct {
	brokers string
	topics  []string
}

type balanceUpdatedPayload struct {
	AccountIDFrom        string  `json:"account_id_from"`
	AccountIDTo          string  `json:"account_id_to"`
	BalanceAccountIDFrom float64 `json:"balance_account_id_from"`
	BalanceAccountIDTo   float64 `json:"balance_account_id_to"`
}

type balanceUpdatedEvent struct {
	Name    string                `json:"Name"`
	Payload balanceUpdatedPayload `json:"Payload"`
}

type BalanceMessageProcessor struct {
	balanceDB *database.BalanceDB
}

func NewConsumer(brokers string, topics []string) *Consumer {
	return &Consumer{brokers: brokers, topics: topics}
}

func NewBalanceMessageProcessor(balanceDB *database.BalanceDB) *BalanceMessageProcessor {
	return &BalanceMessageProcessor{balanceDB: balanceDB}
}

func (c *Consumer) Consume(process func(message *ckafka.Message) error) error {
	config := &ckafka.ConfigMap{
		"bootstrap.servers": c.brokers,
		"group.id":          "balances-consumer",
		"auto.offset.reset": "earliest",
	}
	consumer, err := ckafka.NewConsumer(config)
	if err != nil {
		return fmt.Errorf("failed to create kafka consumer: %w", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics(c.topics, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("kafka consumer error: %v", err)
			continue
		}
		if err := process(msg); err != nil {
			log.Printf("failed to process balance message: %v", err)
		}
	}
}

func (p *BalanceMessageProcessor) Process(message *ckafka.Message) error {
	msg := &balanceUpdatedEvent{}
	if err := json.Unmarshal(message.Value, msg); err != nil {
		return fmt.Errorf("failed to unmarshal kafka message: %w", err)
	}

	payload := msg.Payload
	if payload.AccountIDFrom != "" {
		err := p.balanceDB.CreateOrUpdate(&entity.Balance{AccountID: payload.AccountIDFrom, Balance: payload.BalanceAccountIDFrom})
		if err != nil {
			return err
		}
	}
	if payload.AccountIDTo != "" {
		err := p.balanceDB.CreateOrUpdate(&entity.Balance{AccountID: payload.AccountIDTo, Balance: payload.BalanceAccountIDTo})
		if err != nil {
			return err
		}
	}
	return nil
}
