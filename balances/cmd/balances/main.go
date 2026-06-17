package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/go-sql-driver/mysql"

	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/database"
	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/kafka"
	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/web"
)

func main() {
	dbHost := getenv("MYSQL_HOST", "balances-mysql")
	dbPort := getenv("MYSQL_PORT", "3306")
	dbUser := getenv("MYSQL_USER", "root")
	dbPassword := getenv("MYSQL_PASSWORD", "root")
	dbName := getenv("MYSQL_DATABASE", "balances")
	kafkaBrokers := getenv("KAFKA_BROKERS", "kafka:29092")
	port := getenv("PORT", "3003")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local", dbUser, dbPassword, dbHost, dbPort, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("database connection failed: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("database ping failed: %v", err)
	}

	if err := ensureBalanceSchema(db); err != nil {
		log.Fatalf("failed to initialize balances schema: %v", err)
	}

	if err := seedBalancesData(db); err != nil {
		log.Fatalf("failed to seed balances data: %v", err)
	}

	balanceDB := database.NewBalanceDB(db)
	consumer := kafka.NewConsumer(kafkaBrokers, []string{"balances"})
	processor := kafka.NewBalanceMessageProcessor(balanceDB)

	go func() {
		if err := consumer.Consume(processor.Process); err != nil {
			log.Fatalf("kafka consume error: %v", err)
		}
	}()

	router := chi.NewRouter()
	router.Use(middleware.Logger)
	router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	router.Get("/balances/{account_id}", web.NewBalanceHandler(balanceDB).GetBalance)

	addr := fmt.Sprintf(":%s", port)
	log.Printf("balances service listening on %s", addr)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

func ensureBalanceSchema(db *sql.DB) error {
	statements := []string{
		`CREATE DATABASE IF NOT EXISTS balances`,
		`USE balances`,
		`CREATE TABLE IF NOT EXISTS balances (
		  account_id VARCHAR(255) NOT NULL PRIMARY KEY,
		  balance DOUBLE NOT NULL DEFAULT 0
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute schema statement: %w", err)
		}
	}
	return nil
}

func seedBalancesData(db *sql.DB) error {
	statements := []string{
		`INSERT IGNORE INTO balances.balances (account_id, balance) VALUES ('account-1', 100.0)`,
		`INSERT IGNORE INTO balances.balances (account_id, balance) VALUES ('account-2', 50.0)`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to seed balances data: %w", err)
		}
	}
	return nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
