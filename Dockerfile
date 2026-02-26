# Build para EasyPanel - evita Railpack/mise que falha
FROM node:20-alpine

WORKDIR /app

RUN corepack enable && corepack prepare pnpm@9.15.0 --activate

COPY package.json pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile

COPY src ./src
COPY sql ./sql

ENV NODE_ENV=production

ENTRYPOINT ["pnpm", "exec", "tsx", "src/index.ts"]
CMD ["--mode=full"]
