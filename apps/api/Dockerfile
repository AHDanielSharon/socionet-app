FROM node:20-alpine AS base
WORKDIR /app
RUN apk add --no-cache python3 make g++ ffmpeg curl

FROM base AS deps
COPY package.json tsconfig.json ./
RUN npm install --ignore-scripts

FROM base AS build
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM base AS production
ENV NODE_ENV=production
COPY --from=build /app/dist ./dist
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
EXPOSE 4000
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 CMD curl -f http://localhost:4000/health || exit 1
CMD ["node", "dist/index.js"]
