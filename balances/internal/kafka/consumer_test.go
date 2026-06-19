package kafka

import (
	"database/sql"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/mattn/go-sqlite3"

	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/database"
)

func TestBalanceMessageProcessor_Process(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite in-memory database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE balances (account_id varchar(255) PRIMARY KEY, balance REAL)`)
	if err != nil {
		t.Fatalf("failed to create balances table: %v", err)
	}

	balanceDB := database.NewBalanceDB(db)
	processor := NewBalanceMessageProcessor(balanceDB)

	message := &ckafka.Message{Value: []byte(`{"Name":"BalanceUpdated","Payload":{"account_id_from":"account-1","account_id_to":"account-2","balance_account_id_from":200.0,"balance_account_id_to":150.0}}`)}

	if err := processor.Process(message); err != nil {
		t.Fatalf("expected process to succeed, got error: %v", err)
	}

	balanceFrom, err := balanceDB.FindByAccountID("account-1")
	if err != nil {
		t.Fatalf("failed to find account-1 balance: %v", err)
	}
	if balanceFrom == nil {
		t.Fatal("expected account-1 balance to exist")
	}
	if balanceFrom.Balance != 200.0 {
		t.Fatalf("expected account-1 balance 200.0, got %v", balanceFrom.Balance)
	}

	balanceTo, err := balanceDB.FindByAccountID("account-2")
	if err != nil {
		t.Fatalf("failed to find account-2 balance: %v", err)
	}
	if balanceTo == nil {
		t.Fatal("expected account-2 balance to exist")
	}
	if balanceTo.Balance != 150.0 {
		t.Fatalf("expected account-2 balance 150.0, got %v", balanceTo.Balance)
	}
}
