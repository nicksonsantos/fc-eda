#!/usr/bin/env bash
set -e

host="${MYSQL_HOST:-balances-mysql}"
port="${MYSQL_PORT:-3306}"
user="${MYSQL_USER:-root}"
password="${MYSQL_PASSWORD:-root}"

echo "Waiting for MySQL at ${host}:${port}..."

until mysqladmin ping -h"${host}" -u"${user}" -p"${password}" --silent; do
  sleep 1
  echo 'Waiting for MySQL...'
done

echo "Starting balances service"
exec /balances
