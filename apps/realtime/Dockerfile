FROM node:20-alpine
WORKDIR /app
RUN apk add --no-cache curl
COPY package.json tsconfig.json ./
RUN npm install
COPY . .
RUN npx tsc -p tsconfig.json
EXPOSE 4001
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 CMD curl -f http://localhost:4001/health || exit 1
CMD ["node", "dist/index.js"]
