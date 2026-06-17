package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com.br/devfullcycle/fc-ms-wallet/internal/database"
	"github.com.br/devfullcycle/fc-ms-wallet/internal/event"
	"github.com.br/devfullcycle/fc-ms-wallet/internal/event/handler"
	createaccount "github.com.br/devfullcycle/fc-ms-wallet/internal/usecase/create_account"
	"github.com.br/devfullcycle/fc-ms-wallet/internal/usecase/create_client"
	"github.com.br/devfullcycle/fc-ms-wallet/internal/usecase/create_transaction"
	"github.com.br/devfullcycle/fc-ms-wallet/internal/web"
	"github.com.br/devfullcycle/fc-ms-wallet/internal/web/webserver"
	"github.com.br/devfullcycle/fc-ms-wallet/pkg/events"
	"github.com.br/devfullcycle/fc-ms-wallet/pkg/kafka"
	"github.com.br/devfullcycle/fc-ms-wallet/pkg/uow"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local", "root", "root", "mysql", "3306", "wallet"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := waitForDatabase(db, 30, 1*time.Second); err != nil {
		log.Fatalf("database ping failed: %v", err)
	}

	if err := ensureWalletSchema(db); err != nil {
		log.Fatalf("failed to initialize wallet schema: %v", err)
	}

	if err := seedWalletData(db); err != nil {
		log.Fatalf("failed to seed wallet data: %v", err)
	}

	configMap := ckafka.ConfigMap{
		"bootstrap.servers": "kafka:29092",
		"group.id":          "wallet",
	}
	kafkaProducer := kafka.NewKafkaProducer(&configMap)

	eventDispatcher := events.NewEventDispatcher()
	eventDispatcher.Register("TransactionCreated", handler.NewTransactionCreatedKafkaHandler(kafkaProducer))
	eventDispatcher.Register("BalanceUpdated", handler.NewUpdateBalanceKafkaHandler(kafkaProducer))
	transactionCreatedEvent := event.NewTransactionCreated()
	balanceUpdatedEvent := event.NewBalanceUpdated()

	clientDb := database.NewClientDB(db)
	accountDb := database.NewAccountDB(db)

	ctx := context.Background()
	uow := uow.NewUow(ctx, db)

	uow.Register("AccountDB", func(tx *sql.Tx) interface{} {
		return database.NewAccountDB(db)
	})

	uow.Register("TransactionDB", func(tx *sql.Tx) interface{} {
		return database.NewTransactionDB(db)
	})
	createTransactionUseCase := create_transaction.NewCreateTransactionUseCase(uow, eventDispatcher, transactionCreatedEvent, balanceUpdatedEvent)
	createClientUseCase := create_client.NewCreateClientUseCase(clientDb)
	createAccountUseCase := createaccount.NewCreateAccountUseCase(accountDb, clientDb)

	webserver := webserver.NewWebServer(":8080")

	clientHandler := web.NewWebClientHandler(*createClientUseCase)
	accountHandler := web.NewWebAccountHandler(*createAccountUseCase)
	transactionHandler := web.NewWebTransactionHandler(*createTransactionUseCase)

	webserver.AddHandler("/clients", clientHandler.CreateClient)
	webserver.AddHandler("/accounts", accountHandler.CreateAccount)
	webserver.AddHandler("/transactions", transactionHandler.CreateTransaction)

	fmt.Println("Server is running")
	webserver.Start()
}
func ensureWalletSchema(db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS clients (
      id VARCHAR(255) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      email VARCHAR(255),
      created_at DATETIME NOT NULL
    )`,
		`CREATE TABLE IF NOT EXISTS accounts (
      id VARCHAR(255) PRIMARY KEY,
      client_id VARCHAR(255) NOT NULL,
      balance DOUBLE NOT NULL,
      created_at DATETIME NOT NULL
    )`,
		`CREATE TABLE IF NOT EXISTS transactions (
      id VARCHAR(255) PRIMARY KEY,
      account_id_from VARCHAR(255) NOT NULL,
      account_id_to VARCHAR(255) NOT NULL,
      amount DOUBLE NOT NULL,
      created_at DATETIME NOT NULL
    )`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func seedWalletData(db *sql.DB) error {
	statements := []string{
		`INSERT IGNORE INTO clients (id, name, email, created_at) VALUES ('client-1', 'Alice', 'alice@example.com', NOW())`,
		`INSERT IGNORE INTO accounts (id, client_id, balance, created_at) VALUES ('account-1', 'client-1', 100.0, NOW())`,
		`INSERT IGNORE INTO accounts (id, client_id, balance, created_at) VALUES ('account-2', 'client-1', 50.0, NOW())`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to seed wallet data: %w", err)
		}
	}
	return nil
}

func waitForDatabase(db *sql.DB, attempts int, interval time.Duration) error {
	for i := 0; i < attempts; i++ {
		if err := db.Ping(); err == nil {
			return nil
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("database ping failed after %d attempts", attempts)
}
