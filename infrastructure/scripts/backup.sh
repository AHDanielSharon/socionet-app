#!/bin/bash
DIR="./backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$DIR"
docker-compose exec -T postgres pg_dump -U socionet socionet | gzip > "$DIR/postgres.sql.gz"
echo "Backup saved to $DIR"
