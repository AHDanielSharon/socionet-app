# SOCIONET Deployment Guide

## Prerequisites
- Docker 24+ & Docker Compose v2
- 4GB RAM minimum (8GB recommended)
- 20GB disk space

## Quick Start
```bash
git clone <repo>
cd socionet
cp .env.example .env
# Edit .env with your values
./infrastructure/scripts/setup.sh
docker-compose up -d
# Visit http://localhost
```

## Production Checklist
- [ ] Strong passwords in .env
- [ ] Valid JWT_SECRET (64+ chars random)
- [ ] SMTP configured for emails
- [ ] MinIO credentials changed
- [ ] SSL/TLS certificate configured
- [ ] Backup schedule configured
- [ ] Monitoring alerts set up

## Scaling
- API: `docker-compose scale api=3`
- Realtime: Multiple instances + Redis pub/sub
- Database: PgBouncer for connection pooling
- Storage: Switch to S3 in production

## Backup
```bash
./infrastructure/scripts/backup.sh
```

## Environment Variables
See `.env.example` for all required variables.
