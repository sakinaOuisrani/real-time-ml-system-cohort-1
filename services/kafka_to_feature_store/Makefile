build:
	docker build -t kafka_to_feature_sore .

run: build
	docker run \
	--network=redpanda_network \
	--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	--env-file .env \
	kafka_to_feature_sore  

lint:
	ruff check --fix

format:
	ruff format .