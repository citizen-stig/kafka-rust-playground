run-kafka:
	docker-compose up

run-java-worker:
	mvn clean install && mvn exec:java

run-rust-worker: export RUST_LOG=info
run-rust-worker:
	cd rs && cargo run
