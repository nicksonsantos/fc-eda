FROM golang:1.20

WORKDIR /app

RUN apt-get update && apt-get install -y librdkafka-dev default-mysql-client && rm -rf /var/lib/apt/lists/*

COPY . /app

RUN go mod download
RUN go build -o /balances ./cmd/balances

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 3003

CMD ["/entrypoint.sh"]
