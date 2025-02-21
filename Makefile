SHELL := /bin/bash
.PHONY: env backup install

env:
	@test -f ./.env || { echo ".env file not found!"; exit 1; }
	@. ./.env && \
	test -v DIRTY_PASS || { echo "DIRTY_PASS is not set!"; exit 1; } && \
	test -v DIRTY_USER || { echo "DIRTY_USER is not set!"; exit 1; } && \
	test -v DIRTY_HOST || { echo "DIRTY_HOST is not set!"; exit 1; } && \
	test -v DIRTY_PORT || { echo "DIRTY_PORT is not set!"; exit 1; } && \
	test -v DIRTY_DB || { echo "DIRTY_DB is not set!"; exit 1; } && \
	echo "All required variables are set"

backup: env
	@. ./.env && \
	export PGPASSWORD="${DIRTY_PASS}" && \
	pg_dump \
		--no-password \
		--format=p \
		--blobs \
		--verbose \
		--create \
		--clean \
		--if-exists \
		--column-inserts \
		--encoding=UTF8 \
		-f backup-$$(date +%F).sql \
		-U $$DIRTY_USER \
		-h $$DIRTY_HOST \
		-p $$DIRTY_PORT \
		$$DIRTY_DB

install: env
	@. ./.env && \
	echo -e " \
	CREATE USER $$DIRTY_USER WITH PASSWORD '$$DIRTY_PASS'; \n\
	GRANT ALL PRIVILEGES ON DATABASE $$DIRTY_DB TO $$DIRTY_USER; \n\
	GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $$DIRTY_USER; \n\
	GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $$DIRTY_USER; \n\
	ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO $$DIRTY_USER; \n\
	ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES $$DIRTY_USER;"


