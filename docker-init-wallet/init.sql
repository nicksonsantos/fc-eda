CREATE DATABASE IF NOT EXISTS wallet;
USE wallet;

CREATE TABLE IF NOT EXISTS clients (
  id VARCHAR(255) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255),
  created_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS accounts (
  id VARCHAR(255) PRIMARY KEY,
  client_id VARCHAR(255) NOT NULL,
  balance DOUBLE NOT NULL,
  created_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transactions (
  id VARCHAR(255) PRIMARY KEY,
  account_id_from VARCHAR(255) NOT NULL,
  account_id_to VARCHAR(255) NOT NULL,
  amount DOUBLE NOT NULL,
  created_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO clients (id, name, email, created_at) VALUES
('client-1', 'Alice', 'alice@example.com', NOW());

INSERT IGNORE INTO accounts (id, client_id, balance, created_at) VALUES
('account-1', 'client-1', 100.0, NOW()),
('account-2', 'client-1', 50.0, NOW());
