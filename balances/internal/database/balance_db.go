package database

import (
	"database/sql"
	"fmt"

	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/entity"
)

type BalanceDB struct {
	db *sql.DB
}

func NewBalanceDB(db *sql.DB) *BalanceDB {
	return &BalanceDB{db: db}
}

func (r *BalanceDB) CreateOrUpdate(balance *entity.Balance) error {
	query := `INSERT INTO balances (account_id, balance) VALUES (?, ?) ON DUPLICATE KEY UPDATE balance = VALUES(balance)`
	_, err := r.db.Exec(query, balance.AccountID, balance.Balance)
	if err != nil {
		return fmt.Errorf("create or update balance failed: %w", err)
	}
	return nil
}

func (r *BalanceDB) FindByAccountID(accountID string) (*entity.Balance, error) {
	query := `SELECT account_id, balance FROM balances WHERE account_id = ?`
	row := r.db.QueryRow(query, accountID)

	balance := &entity.Balance{}
	if err := row.Scan(&balance.AccountID, &balance.Balance); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("find balance failed: %w", err)
	}
	return balance, nil
}
