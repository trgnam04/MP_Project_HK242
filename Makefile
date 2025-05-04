DOCKER_COMPOSE_FILE := docker-compose.yml
FINK_TARGET_FILE := FlinkCommerce-1.0-SNAPSHOT.jar
FINK_CLUSTER_PATH := /home/trgnam04/bigdata/flink/bin
PYTHON := python3


# command to check system

check:
	docker ps

up:
	docker compose up -d

down:
	docker compose down

logs-kafka-topics:
	docker exec -it broker kafka-topics --bootstrap-server broker:29092 --list

clean:
	docker compose down -v


check_volume:
	docker volume ls -q 

clear: check_volume	
	docker volume rm $(docker volume ls -q)


# producer command

run-producer:
	cd generate_data/ && $(PYTHON) main.py


# flink command

build-flink:
	mvn clean package && java -jar avro-tools-1.11.0.jar compile schema avro_schema/buy_events.avsc src/main/java	

run-flink:
	flink run target/$(FINK_TARGET_FILE)

start-cluster:
	cd $(FINK_CLUSTER_PATH) && ./start-cluster.sh

stop-cluster:
	cd $(FINK_CLUSTER_PATH) && ./stop-cluster.sh

stop-system: down stop-cluster 

