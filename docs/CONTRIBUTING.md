# Contributing to SOCIONET

## Setup
```bash
yarn install
cp .env.example .env
docker-compose up -d postgres redis minio
yarn dev  # starts all services
```

## Project Structure
- `apps/api` — Express REST API
- `apps/realtime` — Socket.io server  
- `apps/web` — Next.js frontend
- `packages/shared` — Shared types and utilities
- `packages/database` — Database migrations and seeds
- `infrastructure` — Docker, Nginx, monitoring configs

## Code Style
- TypeScript strict mode everywhere
- ESLint + Prettier (run `yarn lint`)
- Conventional commits: `feat:`, `fix:`, `chore:`, `docs:`

## Testing
```bash
yarn test        # run all tests
yarn test:api    # API tests only
yarn test:web    # frontend tests only
```
