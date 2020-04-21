FROM postgres:latest

RUN mkdir -p /docker-entrypoint-initdb.d/
COPY initdb.sql /docker-entrypoint-initdb.d/initdb.sql
