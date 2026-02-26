# Migração contínua (antigo -> novo)

Este pacote define a base para migração **idempotente**, incremental e retomável.

## EasyPanel

Para rodar no EasyPanel sem deixar o PC ligado:

> **Se Railpack falhar com erro de `mise`:** use **Docker** como método de build no EasyPanel (em vez de Nixpacks/Railpack). O `Dockerfile` incluído evita esse problema.

1. **Crie um novo App** no EasyPanel
2. **Conecte o repositório** (Git) ou faça deploy do código
3. **Build:** escolha **Docker** (recomendado) ou Nixpacks
4. **Variáveis de ambiente** (obrigatórias):
   - `SOURCE_MYSQL_URL` — URL do MySQL/MariaDB origem
   - `TARGET_POSTGRES_URL` — URL do Postgres destino
   - `BATCH_SIZE` — (opcional) default 1000

5. **Variáveis para customizar a execução:**
   - `MIGRATION_MODE` — `full` ou `incremental` (default: `full`)
   - `MIGRATION_TABLE` — nome da tabela para rodar só uma (ex: `user`, `company`)

6. **Tipo de deploy:** One-off / Cron Job — o container roda a migration e encerra

O `nixpacks.toml` define o comando de start: `pnpm run start`. O script usa `MIGRATION_MODE` e `MIGRATION_TABLE` quando não passados via argumentos.

---

## Ordem de execução (dependência real do `novo.sql`)

- **L1 (dimensões/base):** `civil_status`, `course`, `education_level`, `educational_institution`, `gender`, `semester`, `shift`, `state`, `user`
- **L2 (mestres):** `company`, `institutions`, `student`
- **L3 (cadastros vinculados):** `company_representative`, `company_supervisor`, `institution_representative`, `institution_supervisor`
- **L4 (transacional principal):** `internship_commitment_term`, `internship_opportunity`
- **L5 (assinaturas/docs):** `signed_internship_commitment_term`

> `account`, `session`, `authenticator`, `verificationToken`, `password_reset_tokens` não são prioridade de negócio para cutover de dados operacionais.

## Fluxo

1. Rodar `sql/control_tables.sql` no banco novo.
2. Executar cargas FULL por nível (L1 -> L5).
3. Ativar INCREMENTAL (por `updated_at`/checkpoint).
4. Rodar validadores (`count`, `fk`, amostragem por hash).
5. Cutover com delta final.

## Princípios obrigatórios

- `UPSERT` em todas as cargas (nunca `insert` puro em jobs recorrentes).
- `migration_map` para rastrear `legacy_id -> new_id`.
- checkpoint por job para retomar sem duplicar.
- erro detalhado em `migration_errors` com payload.
