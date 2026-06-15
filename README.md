# Wallet Core + Balances Microservice

This repository contains the existing Wallet Core service and a new `Balances` microservice implemented as a Kafka consumer.

## Architecture

- `Wallet Core` (port `8080`)
  - Handles clients, accounts, and transactions
  - Persists data in MySQL (`wallet` database)
  - Produces Kafka events for `TransactionCreated` and `BalanceUpdated`
- `Balances` service (port `3003`)
  - Consumes Kafka `balances` events
  - Persists balances in a separate MySQL database (`balances`)
  - Exposes `GET /balances/{account_id}`

## Services

- `wallet-core`: Go web service running on port `8080`
- `balances`: Kafka consumer service running on port `3003`
- `mysql`: MySQL instance for Wallet Core database
- `balances-mysql`: MySQL instance for Balances service database
- `zookeeper`, `kafka`, `control-center`: Kafka infrastructure

## Run locally

1. Start all services:

```bash
docker compose up --build
```

2. Wait until the services are healthy.

3. Use the Wallet Core API to create clients, accounts and transactions.

4. Query balances:

```bash
curl http://localhost:3003/balances/<account_id>
```

## Endpoints

### Wallet Core

- `POST /clients`
  - Exemplo:
    ```json
    {
      "name": "John Doe",
      "email": "john@example.com"
    }
    ```
- `POST /accounts`
- `POST /transactions`

### Balances

- `GET /balances/{account_id}`
- `GET /health`

## Notes

- Only the root `README.md` remains as documentation.
- Extra markdown files were removed to keep documentation consolidated.
