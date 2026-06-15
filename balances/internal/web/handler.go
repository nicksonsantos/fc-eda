package web

import (
	"encoding/json"
	"net/http"

	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/database"
	"github.com.br/devfullcycle/fc-ms-wallet/balances/internal/entity"
	"github.com/go-chi/chi/v5"
)

type BalanceHandler struct {
	balanceDB *database.BalanceDB
}

func NewBalanceHandler(balanceDB *database.BalanceDB) *BalanceHandler {
	return &BalanceHandler{balanceDB: balanceDB}
}

func (h *BalanceHandler) GetBalance(w http.ResponseWriter, r *http.Request) {
	accountID := chi.URLParam(r, "account_id")
	if accountID == "" {
		http.Error(w, "account_id is required", http.StatusBadRequest)
		return
	}

	balance, err := h.balanceDB.FindByAccountID(accountID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if balance == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&entity.Balance{AccountID: accountID, Balance: 0})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(balance)
}
