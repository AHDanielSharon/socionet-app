#!/bin/bash
set -e
echo "Setting up SOCIONET..."
if [ ! -f .env ]; then cp .env.example .env; echo "Created .env - fill in your values!"; fi
docker-compose up -d postgres redis minio minio-setup
until docker-compose exec -T postgres pg_isready -U socionet 2>/dev/null; do sleep 2; done
echo "PostgreSQL ready"
echo "Setup complete! Run: docker-compose up -d"
