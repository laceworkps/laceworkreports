# Usage: make COMMAND
#
# Commands
#   help                    Show help message.
#   postgres-console        Login to postgres interactive terminal.
#   postgres-create-db      Create postgres database.
#   build                   Build application.
#   service                 Start up application.
#   stop                    Stop running application.
#   clean                   Stop app and remove containers.
#   clean-all               Remove app, remove containers and volumes.
#
include .env

PSQL = PGPASSWORD=$(POSTGRES_PASSWORD) \
	docker exec -i $(POSTGRES_CONTAINER) \
	psql -h $(POSTGRES_HOST) -p 5432 -U $(POSTGRES_USER)

help:
	@head -12 Makefile

postgres-console:
	PGPASSWORD=$(POSTGRES_PASSWORD) \
	docker exec -it $(POSTGRES_CONTAINER) \
	psql -h $(POSTGRES_HOST) -p 5432 -U $(POSTGRES_USER) -d $(POSTGRES_DBNAME)

postgres-create-db:
	$(PSQL) -c "CREATE DATABASE $(POSTGRES_DBNAME)"

laceworkreports-console:
	docker exec -it $(LACEWORKREPORTS_CONTAINER) \
	/bin/bash

build:
	docker-compose up -d
	make postgres-create-db

start:
	docker-compose up -d

stop:
	docker-compose down

clean:
	make stop
	docker system prune -af

clean-all:
	make clean
	docker volume prune
