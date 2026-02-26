-- Controle de execução
create table if not exists migration_runs (
  id bigserial primary key,
  job_name text not null,
  mode text not null check (mode in ('full', 'incremental')),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running' check (status in ('running', 'success', 'failed')),
  rows_read integer not null default 0,
  rows_inserted integer not null default 0,
  rows_updated integer not null default 0,
  rows_failed integer not null default 0,
  error_message text
);

-- Ponto de retomada por job
create table if not exists migration_checkpoints (
  job_name text primary key,
  last_legacy_id text,
  last_updated_at timestamptz,
  updated_at timestamptz not null default now()
);

-- Mapa entre IDs legado e novo
create table if not exists migration_map (
  entity text not null,
  legacy_id text not null,
  new_id text not null,
  source_hash text,
  migrated_at timestamptz not null default now(),
  primary key (entity, legacy_id),
  unique (entity, new_id)
);

create index if not exists idx_migration_map_entity_new_id on migration_map (entity, new_id);

-- Falhas detalhadas
create table if not exists migration_errors (
  id bigserial primary key,
  run_id bigint references migration_runs(id) on delete set null,
  job_name text not null,
  legacy_id text,
  stage text not null check (stage in ('extract', 'transform', 'load', 'validate')),
  error_message text not null,
  payload jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_migration_errors_run_id on migration_errors (run_id);
create index if not exists idx_migration_errors_job_name on migration_errors (job_name);
