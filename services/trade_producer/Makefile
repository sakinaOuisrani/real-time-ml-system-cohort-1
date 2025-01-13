build:
	docker build -t trade_producer .

run: build
	docker run \
	--network=redpanda_network \
	--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	trade_producer  

lint:
	ruff check --fix

format:
	ruff format .