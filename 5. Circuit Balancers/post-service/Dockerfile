FROM oven/bun:latest

WORKDIR /app

COPY package.json bun.lock ./

RUN bun install --frozen-lockfile

COPY . .

RUN bun build ./src/index.ts --outdir ./dist --target node

CMD ["node", "dist/index.js"]
