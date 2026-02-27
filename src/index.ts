import "dotenv/config";
import mysql from "mysql2/promise";
import { Pool } from "pg";
import { uuidv7 } from "uuidv7";

type Job = {
  key: string;
  level: number;
  sourceTable: string;
  pk: string;
  extractSql: (mode: "full" | "incremental") => string;
  upsert: (row: any, ctx: Ctx) => Promise<boolean>;
};

type Ctx = {
  source: mysql.Pool;
  target: Pool;
  mode: "full" | "incremental";
  tableFilter?: string;
  stateByUf: Map<string, string>;
};

const sourceUrl = process.env.SOURCE_MYSQL_URL;
const targetUrl = process.env.TARGET_POSTGRES_URL;
if (!sourceUrl || !targetUrl) {
  console.error("Missing SOURCE_MYSQL_URL or TARGET_POSTGRES_URL");
  process.exit(1);
}

const args = process.argv.slice(2);
const mode = (args.find((a) => a.startsWith("--mode="))?.split("=")[1] ??
  process.env.MIGRATION_MODE ??
  "full") as "full" | "incremental";
const tableFilter = args.includes("--table")
  ? args[args.indexOf("--table") + 1]
  : (process.env.MIGRATION_TABLE ?? undefined);
const batchSize = Number(process.env.BATCH_SIZE || 1000);

const source = mysql.createPool(sourceUrl);
const target = new Pool({ connectionString: targetUrl });

/** Gera UUIDv7 para IDs no Postgres (todas as tabelas usam UUID) */
function newId() {
  return uuidv7();
}

function normalizeZip(zip?: string | null) {
  if (!zip) return null;
  const digits = String(zip).replace(/\D/g, "");
  if (digits.length === 8) return `${digits.slice(0, 5)}-${digits.slice(5)}`;
  return digits.slice(0, 9);
}

function normalizeCpf(cpf?: string | null) {
  if (!cpf) return null;
  return String(cpf).replace(/\D/g, "");
}

function normalizePhone(phone?: string | null) {
  if (!phone) return null;
  return String(phone).replace(/\s+/g, "").trim();
}

function normalizePhoneOrDefault(
  phone?: string | null,
  fallback = "0000000000",
) {
  const normalized = normalizePhone(phone);
  if (!normalized) return fallback;
  return normalized;
}

function mapPaymentFrequency(value?: string | null) {
  const v = (value ?? "").toString().trim().toLowerCase();
  if (!v) return null;
  const map: Record<string, string> = {
    mensalmente: "monthly",
    mensal: "monthly",
    semanalmente: "weekly",
    semanal: "weekly",
    hora: "hourly",
    hourly: "hourly",
    daily: "daily",
    weekly: "weekly",
    biweekly: "biweekly",
    monthly: "monthly",
  };
  return map[v] ?? null;
}

function toStringArray(value?: unknown): string[] | null {
  if (value == null) return null;
  if (Array.isArray(value)) {
    const cleaned = value
      .map((v) => sanitizeOptionalString(String(v)))
      .filter((v): v is string => !!v);
    return cleaned.length ? cleaned : null;
  }
  const raw = sanitizeOptionalString(String(value));
  if (!raw) return null;
  const parts = raw
    .split(/,|;/)
    .map((p) => p.trim())
    .filter(Boolean);
  return parts.length ? parts : [raw];
}

function mapLanguageLevel(value?: string | null) {
  const v = (value ?? "").toString().trim().toLowerCase();
  if (!v) return null;
  if (["nenhum", "básico", "basico"].includes(v)) return "beginner";
  if (["intermediário", "intermediario"].includes(v)) return "intermediate";
  if (["avançado", "avancado"].includes(v)) return "advanced";
  if (v === "native") return "native";
  return "beginner";
}

async function getGenderIdFromLegacy(value: any) {
  if (value == null) return null;

  // If numeric or numeric-like, use migration map
  const asNumber = Number(value);
  if (!Number.isNaN(asNumber) && Number.isFinite(asNumber)) {
    const mapped = await mapGet("gender", value);
    if (mapped) return mapped;
  }

  const raw = sanitizeRequiredString(value).toLowerCase();
  if (!raw) return null;

  const genderMap: Record<string, string> = {
    masculino: "Masculino",
    feminino: "Feminino",
    outro: "Outro",
    ambos: "Outro",
    m: "Masculino",
    f: "Feminino",
    o: "Outro",
  };

  const mappedName = genderMap[raw] ?? sanitizeRequiredString(value);

  const res = await target.query(
    `SELECT id FROM gender WHERE lower(name) = lower($1) LIMIT 1`,
    [mappedName],
  );
  if (res.rows && res.rows.length > 0) return res.rows[0].id as string;

  return null;
}

async function getDefaultGenderId() {
  const res = await target.query(`SELECT id FROM gender ORDER BY name LIMIT 1`);
  return res.rows?.[0]?.id ?? null;
}

async function getDefaultSemesterId() {
  const res = await target.query(
    `SELECT id FROM semester ORDER BY name NULLS LAST LIMIT 1`,
  );
  return res.rows?.[0]?.id ?? null;
}

function mapUserRole(role?: string | null) {
  const r = (role ?? "").toString().trim().toLowerCase();
  if (r === "undefined" || r === "") return "student";
  if (["student", "company", "institution", "admin"].includes(r)) return r;
  return "student";
}

/**
 * Remove null bytes (0x00) from strings. PostgreSQL does not allow null bytes in UTF8 text.
 * Returns null for null/undefined; otherwise returns cleaned string.
 */
function stripNullBytes(value: string | null | undefined): string | null {
  if (value == null) return null;
  const s = typeof value === "string" ? value : String(value);
  return s.replace(/\u0000/g, "");
}

/**
 * Sanitize optional string: strip null bytes, trim, return null if empty.
 */
function sanitizeOptionalString(
  value: string | null | undefined,
): string | null {
  const cleaned = stripNullBytes(value);
  if (cleaned == null) return null;
  const trimmed = cleaned.trim();
  return trimmed === "" ? null : trimmed;
}

/**
 * Sanitize required string: strip null bytes, trim, fallback to empty string.
 */
function sanitizeRequiredString(value: string | null | undefined): string {
  const cleaned = stripNullBytes(value);
  if (cleaned == null) return "";
  return cleaned.trim();
}

async function ensureControlTables() {
  const sql = await (
    await import("node:fs/promises")
  ).readFile(new URL("../sql/control_tables.sql", import.meta.url), "utf8");
  await target.query(sql);
}

async function startRun(jobName: string) {
  const r = await target.query(
    `insert into migration_runs(job_name, mode) values ($1,$2) returning id`,
    [jobName, mode],
  );
  return r.rows[0].id as number;
}

async function endRun(
  runId: number,
  status: "success" | "failed",
  err?: string,
) {
  await target.query(
    `update migration_runs set status=$2, finished_at=now(), error_message=$3 where id=$1`,
    [runId, status, err ?? null],
  );
}

async function mapPut(
  entity: string,
  legacyId: string | number,
  newId: string,
) {
  await target.query(
    `insert into migration_map(entity, legacy_id, new_id, migrated_at)
     values ($1,$2,$3,now())
     on conflict (entity, legacy_id) do update set new_id=excluded.new_id, migrated_at=now()`,
    [entity, String(legacyId), newId],
  );
}

async function mapGet(
  entity: string,
  legacyId: string | number | null | undefined,
) {
  if (legacyId === null || legacyId === undefined) return null;
  const r = await target.query(
    `select new_id from migration_map where entity=$1 and legacy_id=$2`,
    [entity, String(legacyId)],
  );
  return r.rows[0]?.new_id ?? null;
}

async function resolveId(
  entity: string,
  legacyId: string | number | null | undefined,
) {
  const existing = await mapGet(entity, legacyId);
  return existing ?? newId();
}

// --- Legacy key helpers (INT + UUID coexistence) ---
function normalizeLegacyId(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

/**
 * Stores BOTH legacy identifiers (e.g. INT co_seq_* and UUID char(36))
 * pointing to the same new_id.
 */
async function mapPutBoth(
  entity: string,
  legacyInt: unknown,
  legacyUuid: unknown,
  newId: string,
) {
  const a = normalizeLegacyId(legacyInt);
  const b = normalizeLegacyId(legacyUuid);
  if (a) await mapPut(entity, a, newId);
  if (b && b !== a) await mapPut(entity, b, newId);
}

/** Try multiple legacy candidates until one resolves in migration_map. */
async function mapGetAny(entity: string, ...candidates: unknown[]) {
  for (const c of candidates) {
    const v = normalizeLegacyId(c);
    if (!v) continue;
    const id = await mapGet(entity, v);
    if (id) return id;
  }
  return null;
}

async function bootstrapStates() {
  const m = new Map<string, string>();
  const r = await target.query(`select id, acronym from state`);
  for (const row of r.rows) m.set(String(row.acronym).toUpperCase(), row.id);
  return m;
}

const dimUpsert = (table: string) => async (row: any, _ctx?: Ctx) => {
  const id = await resolveId(table, row.id);
  await target.query(
    `insert into ${table}(id,name,created_at,updated_at)
     values ($1,$2,coalesce($3,now()),coalesce($4,now()))
     on conflict (id) do update set name=excluded.name, updated_at=now()`,
    [id, row.name, row.created_at ?? null, row.updated_at ?? null],
  );
  await mapPut(table, row.id, id);
  return true;
};

const jobs: Job[] = [
  {
    key: "state",
    level: 1,
    sourceTable: "tb_estados",
    pk: "id",
    extractSql: () =>
      `select id, estado as name, uf as acronym from tb_estados`,
    upsert: async (row) => {
      const id = await resolveId("state", row.id);
      await target.query(
        `insert into state(id,name,acronym,created_at,updated_at)
      values ($1,$2,$3,now(),now())
      on conflict (id) do update set name=excluded.name, acronym=excluded.acronym, updated_at=now()`,
        [id, row.name, row.acronym],
      );
      await mapPut("state", row.id, id);
      return true;
    },
  },
  {
    key: "gender",
    level: 1,
    sourceTable: "tb_sexo",
    pk: "id",
    extractSql: () =>
      `select id, sexo as name, created_at, created_at as updated_at from tb_sexo`,
    upsert: dimUpsert("gender"),
  },
  {
    key: "civil_status",
    level: 1,
    sourceTable: "tb_estado_civil",
    pk: "id",
    extractSql: () =>
      `select id, estado_civil as name, created_at, created_at as updated_at from tb_estado_civil`,
    upsert: dimUpsert("civil_status"),
  },
  {
    key: "education_level",
    level: 1,
    sourceTable: "tb_escolaridade",
    pk: "id",
    extractSql: () =>
      `select id, nivel as name, null as created_at, null as updated_at from tb_escolaridade`,
    upsert: dimUpsert("education_level"),
  },
  {
    key: "course",
    level: 1,
    sourceTable: "tb_confcurso",
    pk: "co_seq_confcurso",
    extractSql: () =>
      `select co_seq_confcurso as id, no_curso as name, dt_publicado as created_at, dt_publicado as updated_at from tb_confcurso`,
    upsert: dimUpsert("course"),
  },
  {
    key: "educational_institution",
    level: 1,
    sourceTable: "tb_confinstituicao",
    pk: "co_seq_confinstituicao",
    extractSql: () =>
      `select co_seq_confinstituicao as id, no_instituicao as name, dt_publicado as created_at, dt_publicado as updated_at from tb_confinstituicao`,
    upsert: dimUpsert("educational_institution"),
  },
  {
    key: "semester",
    level: 1,
    sourceTable: "tb_semestre",
    pk: "id",
    extractSql: () =>
      `select id, semestre as name, null as created_at, null as updated_at from tb_semestre`,
    upsert: dimUpsert("semester"),
  },
  {
    key: "shift",
    level: 1,
    sourceTable: "tb_turno",
    pk: "id",
    extractSql: () =>
      `select id, turno as name, created_at, created_at as updated_at from tb_turno`,
    upsert: dimUpsert("shift"),
  },

  {
    key: "user",
    level: 1,
    sourceTable: "tb_usuario",
    pk: "co_seq_usuario",
    extractSql: () => `select * from tb_usuario`,
    upsert: async (row) => {
      const id = await resolveId("user", row.co_seq_usuario);
      const baseEmail = sanitizeRequiredString(
        row.email ?? `${id}@legacy.local`,
      ).toLowerCase();
      await target.query(
        `insert into "user"(id,name,email,password,"emailVerified",image,role,created_at,updated_at)
      values ($1,$2,$3,$4,null,$5,$6,coalesce($7,now()),coalesce($8,now()))
      on conflict (email) do update set name=excluded.name,password=excluded.password,image=excluded.image,role=excluded.role,updated_at=now()`,
        [
          id,
          sanitizeRequiredString(row.name ?? row.ds_login ?? "Sem nome"),
          baseEmail,
          row.password_hash ?? row.ds_senha ?? "legacy",
          sanitizeOptionalString(row.image),
          mapUserRole(row.role),
          row.created_at ?? null,
          row.updated_at ?? null,
        ],
      );
      // Map both numeric PK and legacy UUID (tb_usuario.id is char(36))
      await mapPutBoth("user", row.co_seq_usuario, row.id, id);
      return true;
    },
  },

  {
    key: "company",
    level: 2,
    sourceTable: "tb_empresa",
    pk: "co_seq_empresa",
    extractSql: () => `select * from tb_empresa`,
    upsert: async (row, ctx) => {
      // Canonical legacy key = co_seq_empresa (INT). Also map UUID tb_empresa.id if present.
      const id = await resolveId("company", row.co_seq_empresa);
      const userId = await mapGet("user", row.user_id);
      const stateId =
        ctx.stateByUf.get(String(row.ds_uf || "").toUpperCase()) ?? null;
      if (!userId || !stateId) return false;
      await target.query(
        `insert into company(id,user_id,notes,legal_name,trade_name,activities,cnpj_number,state_registration,address,city,state_id,zip_code,phone,whatsapp,created_at,updated_at)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,coalesce($15,now()),coalesce($16,now()))
      on conflict (id) do update set user_id=excluded.user_id,legal_name=excluded.legal_name,trade_name=excluded.trade_name,updated_at=now()`,
        [
          id,
          userId,
          sanitizeOptionalString(row.ds_obs_futura_emp),
          sanitizeRequiredString(row.ds_razao_social),
          sanitizeOptionalString(row.ds_nome_fantasia),
          sanitizeOptionalString(row.ds_atividade),
          sanitizeRequiredString(row.nu_cnpj),
          sanitizeOptionalString(row.ds_insc_est),
          sanitizeOptionalString(row.ds_endereco),
          sanitizeOptionalString(row.ds_cidade),
          stateId,
          normalizeZip(row.nu_cep),
          normalizePhoneOrDefault(row.nu_telefone),
          sanitizeOptionalString(normalizePhone(row.nu_fax)),
          row.created_at,
          row.updated_at,
        ],
      );
      await mapPutBoth("company", row.co_seq_empresa, row.id, id);
      return true;
    },
  },

  {
    key: "institutions",
    level: 2,
    sourceTable: "tb_instituicao",
    pk: "co_seq_instituicao",
    extractSql: () => `select * from tb_instituicao`,
    upsert: async (row, ctx) => {
      // Canonical legacy key = co_seq_instituicao (INT). Also map UUID tb_instituicao.id if present.
      const id = await resolveId("institutions", row.co_seq_instituicao);
      const userId = await mapGet("user", row.user_id);
      const stateId =
        ctx.stateByUf.get(String(row.ds_uf || "").toUpperCase()) ?? null;
      if (!userId || !stateId) return false;
      await target.query(
        `insert into institutions(id,user_id,notes,legal_name,trade_name,activities,cnpj_number,state_registration,address,city,state_id,zip_code,phone,whatsapp,created_at,updated_at)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,coalesce($15,now()),coalesce($16,now()))
      on conflict (id) do update set user_id=excluded.user_id,legal_name=excluded.legal_name,trade_name=excluded.trade_name,updated_at=now()`,
        [
          id,
          userId,
          sanitizeOptionalString(row.ds_obs_futura_inst),
          sanitizeRequiredString(row.ds_razao_social),
          sanitizeOptionalString(row.ds_nome_fantasia),
          sanitizeOptionalString(row.ds_atividade),
          sanitizeRequiredString(row.nu_cnpj),
          sanitizeOptionalString(row.ds_insc_est),
          sanitizeOptionalString(row.ds_endereco),
          sanitizeOptionalString(row.ds_cidade),
          stateId,
          normalizeZip(row.nu_cep),
          normalizePhoneOrDefault(row.nu_telefone),
          sanitizeOptionalString(normalizePhone(row.nu_fax)),
          row.created_at,
          row.updated_at,
        ],
      );
      await mapPutBoth("institutions", row.co_seq_instituicao, row.id, id);
      return true;
    },
  },

  {
    key: "student",
    level: 2,
    sourceTable: "tb_estudante",
    pk: "id",
    extractSql: () => `select * from tb_estudante`,
    upsert: async (row) => {
      const id = await resolveId("student", row.id);
      const userId = await mapGet("user", row.usuario_id);
      if (!userId) return false;
      let genderId = await getGenderIdFromLegacy(row.genero);
      if (!genderId) {
        genderId = await getDefaultGenderId();
      }
      const civilId = await mapGet("civil_status", row.estado_civil_id);
      const stateId = await mapGet("state", row.estado_id);
      const eduId = await mapGet("education_level", row.nivel_escolaridade_id);
      const courseId = await mapGet("course", row.curso_id);
      const eduInstId = await mapGet(
        "educational_institution",
        row.instituicao_id,
      );
      let semesterId = await mapGet("semester", row.semestre_id);
      if (!semesterId) {
        semesterId = await getDefaultSemesterId();
      }
      const shiftId = await mapGet("shift", row.turno_id);
      await target.query(
        `insert into student(id,user_id,notes,full_name,birth_date,cpf_number,rg_number,issue_agency,has_driver_license,gender_id,civil_status_id,has_disability,disability_type,father_name,mother_name,address,city,state_id,zip_code,phone,whatsapp,education_level_id,course_id,educational_institution_id,has_oab_license,enrollment,semester_id,shift_id,available_shift_id,english_level,spanish_level,french_level,other_languages,improvement_courses,it_courses,created_at,updated_at)
      values ($1,$2,$3,$4,coalesce($5,now()::date),$6,$7,$8,coalesce($9,false),$10,$11,coalesce($12,false),$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,coalesce($25,false),$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,coalesce($36,now()),coalesce($37,now()))
      on conflict (id) do update set full_name=excluded.full_name,updated_at=now()`,
        [
          id,
          userId,
          sanitizeOptionalString(row.notas),
          sanitizeRequiredString(row.nome_completo),
          row.data_nascimento,
          normalizeCpf(row.cpf),
          sanitizeRequiredString(row.rg),
          sanitizeOptionalString(row.orgao_expedidor),
          !!row.possui_cnh,
          genderId,
          civilId,
          !!row.possui_deficiencia,
          sanitizeOptionalString(row.tipo_deficiencia),
          sanitizeOptionalString(row.nome_pai),
          sanitizeOptionalString(row.nome_mae),
          sanitizeOptionalString(row.endereco),
          sanitizeOptionalString(row.cidade),
          stateId,
          normalizeZip(row.cep),
          sanitizeRequiredString(normalizePhone(row.telefone)) || null,
          sanitizeOptionalString(normalizePhone(row.whatsapp)),
          eduId,
          courseId,
          eduInstId,
          !!row.possui_oab,
          sanitizeOptionalString(row.matricula),
          semesterId,
          shiftId,
          shiftId,
          mapLanguageLevel(row.ingles),
          mapLanguageLevel(row.espanhol),
          mapLanguageLevel(row.frances),
          toStringArray(row.outro_idioma),
          toStringArray(row.ImprovementCourse),
          toStringArray(row.ITCourse),
          row.createdAt,
          row.updatedAt,
        ],
      );
      await mapPut("student", row.id, id);
      return true;
    },
  },

  {
    key: "company_supervisor",
    level: 3,
    sourceTable: "supervisor_empresas",
    pk: "id",
    extractSql: () => `select * from supervisor_empresas`,
    upsert: async (row) => {
      const id = await resolveId("company_supervisor", row.id);
      const companyId = await mapGet("company", row.empresa_id);
      if (!companyId) return false;
      await target.query(
        `insert into company_supervisor(id,company_id,full_name,cpf_number,rg_number,issuing_authority,phone,whatsapp,position,created_at,updated_at)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,coalesce($10,now()),coalesce($11,now()))
      on conflict (id) do update set full_name=excluded.full_name,position=excluded.position,updated_at=now()`,
        [
          id,
          companyId,
          sanitizeRequiredString(row.nomeCompleto),
          normalizeCpf(row.cpf),
          sanitizeOptionalString(row.rg),
          sanitizeOptionalString(row.orgaoEmissor),
          sanitizeOptionalString(normalizePhone(row.telefone)),
          sanitizeOptionalString(normalizePhone(row.celular)),
          sanitizeOptionalString(row.cargo),
          row.created_at,
          row.updated_at,
        ],
      );
      await mapPut("company_supervisor", row.id, id);
      return true;
    },
  },

  {
    key: "company_representative",
    level: 3,
    sourceTable: "representante_empresas",
    pk: "id",
    extractSql: () => `select * from representante_empresas`,
    upsert: async (row) => {
      const companyId = await mapGet("company", row.empresa_id);
      if (!companyId) return false;
      const id = await resolveId("company_representative", row.id);
      await target.query(
        `insert into company_representative(id,company_id,full_name,cpf_number,rg_number,issuing_authority,phone,whatsapp,position,created_at,updated_at)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,coalesce($10,now()),coalesce($11,now()))
      on conflict (id) do update set full_name=excluded.full_name,position=excluded.position,updated_at=now()`,
        [
          id,
          companyId,
          sanitizeRequiredString(row.nomeCompleto),
          normalizeCpf(row.cpf),
          sanitizeOptionalString(row.rg),
          sanitizeOptionalString(row.orgaoEmissor),
          sanitizeOptionalString(normalizePhone(row.telefone)),
          sanitizeOptionalString(normalizePhone(row.celular)),
          sanitizeOptionalString(row.cargo),
          row.created_at,
          row.updated_at,
        ],
      );
      await mapPut("company_representative", row.id, id);
      return true;
    },
  },

  {
    key: "institution_supervisor",
    level: 3,
    sourceTable: "supervisor_instituicaos",
    pk: "id",
    extractSql: () => `select * from supervisor_instituicaos`,
    upsert: async (row) => {
      const institutionId = await mapGet("institutions", row.instituicao_id);
      if (!institutionId) return false;
      const id = await resolveId("institution_supervisor", row.id);
      await target.query(
        `insert into institution_supervisor(id,institution_id,full_name,cpf_number,rg_number,issuing_authority,phone,whatsapp,position,created_at,updated_at)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,coalesce($10,now()),coalesce($11,now()))
      on conflict (id) do update set full_name=excluded.full_name,position=excluded.position,updated_at=now()`,
        [
          id,
          institutionId,
          sanitizeRequiredString(row.nomeCompleto),
          normalizeCpf(row.cpf),
          sanitizeOptionalString(row.rg),
          sanitizeOptionalString(row.orgaoEmissor),
          sanitizeOptionalString(normalizePhone(row.telefone)),
          sanitizeOptionalString(normalizePhone(row.celular)),
          sanitizeOptionalString(row.cargo),
          row.created_at,
          row.updated_at,
        ],
      );
      await mapPut("institution_supervisor", row.id, id);
      return true;
    },
  },

  {
    key: "institution_representative",
    level: 3,
    sourceTable: "representante_instituicaos",
    pk: "id",
    extractSql: () => `select * from representante_instituicaos`,
    upsert: async (row) => {
      const institutionId = await mapGet("institutions", row.instituicao_id);
      if (!institutionId) return false;
      const id = await resolveId("institution_representative", row.id);
      await target.query(
        `insert into institution_representative(id,institution_id,full_name,cpf_number,rg_number,issuing_authority,phone,whatsapp,position,created_at,updated_at)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,coalesce($10,now()),coalesce($11,now()))
      on conflict (id) do update set full_name=excluded.full_name,position=excluded.position,updated_at=now()`,
        [
          id,
          institutionId,
          sanitizeRequiredString(row.nomeCompleto),
          normalizeCpf(row.cpf),
          sanitizeOptionalString(row.rg),
          sanitizeOptionalString(row.orgaoEmissor),
          sanitizeOptionalString(normalizePhone(row.telefone)),
          sanitizeOptionalString(normalizePhone(row.celular)),
          sanitizeOptionalString(row.cargo),
          row.created_at,
          row.updated_at,
        ],
      );
      await mapPut("institution_representative", row.id, id);
      return true;
    },
  },

  {
    key: "internship_commitment_term",
    level: 4,
    sourceTable: "tb_termo",
    pk: "co_seq_termo",
    extractSql: () => `select * from tb_termo`,
    upsert: async (row) => {
      // Canonical legacy key = co_seq_termo (INT). Also map UUID tb_termo.id if present.
      const id = await resolveId(
        "internship_commitment_term",
        row.co_seq_termo,
      );

      // In legacy, tb_termo.empresa_id / instituicao_id are UUID (char(36))
      // while other tables may reference company/institution by INT. We map BOTH.
      const companyId = await mapGetAny("company", row.empresa_id);
      const companySupervisorId = await mapGet(
        "company_supervisor",
        row.supervisor_empresa_id,
      );
      const companyRepresentativeId = await mapGet(
        "company_representative",
        row.representante_empresa_id,
      );
      const institutionId = await mapGetAny("institutions", row.instituicao_id);
      const institutionSupervisorId = await mapGet(
        "institution_supervisor",
        row.supervisor_instituicao_id,
      );
      const institutionRepresentativeId = await mapGet(
        "institution_representative",
        row.representante_instituicao_id,
      );
      const studentId = await mapGet("student", row.estudante_id);
      if (
        !companyId ||
        !companySupervisorId ||
        !companyRepresentativeId ||
        !institutionId ||
        !institutionSupervisorId ||
        !institutionRepresentativeId ||
        !studentId
      )
        return false;
      const publicNumber = String(row.co_seq_termo ?? "")
        .padStart(6, "0")
        .slice(-6);
      await target.query(
        `insert into internship_commitment_term(
      id,public_number,notes,company_id,company_supervisor_id,company_supervisor_position,company_representative_id,company_representative_position,institution_id,institution_supervisor_id,institution_supervisor_position,institution_representative_id,institution_representative_position,student_id,first_activity,second_activity,start_commitment_date,end_commitment_date,days_and_hours_per_week,stipend_amount,payment_frequency,transportation_allowance_amount,term_date,first_extension_date,second_extension_date,third_extension_date,termination_date,created_at,updated_at)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,coalesce($28,now()),coalesce($29,now()))
      on conflict (id) do update set updated_at=now(), notes=excluded.notes`,
        [
          id,
          publicNumber,
          row.notas,
          companyId,
          companySupervisorId,
          row.cargo_supervisor_empresa,
          companyRepresentativeId,
          row.cargo_representante_empresa,
          institutionId,
          institutionSupervisorId,
          row.cargo_supervisor_instituicao,
          institutionRepresentativeId,
          row.cargo_representante_instituicao,
          studentId,
          row.paragrafo_a,
          row.paragrafo_b,
          row.data_inicio,
          row.data_fim,
          row.hora_especial,
          row.valor_estagio,
          mapPaymentFrequency(row.taxa_pagamento),
          row.vale_transporte,
          row.data,
          row.prorrogacao1,
          row.prorrogacao2,
          row.prorrogacao3,
          row.rescisao,
          row.created_at,
          row.updated_at,
        ],
      );
      await mapPutBoth(
        "internship_commitment_term",
        row.co_seq_termo,
        row.id,
        id,
      );
      return true;
    },
  },

  {
    key: "signed_internship_commitment_term",
    level: 5,
    sourceTable: "termos",
    pk: "id",
    extractSql: () => `select * from termos`,
    upsert: async (row) => {
      const id = await resolveId("signed_internship_commitment_term", row.id);
      const termId = await mapGetAny(
        "internship_commitment_term",
        row.id,
        row.termoId,
      );
      const companyId = await mapGetAny(
        "company",
        row.empresaId,
        row.empresa_id,
      );
      const studentId = await mapGet("student", row.estudanteId);
      if (!termId || !companyId || !studentId) return false;
      await target.query(
        `insert into signed_internship_commitment_term(id,public_id,internship_commitment_term_id,company_id,student_id,pdf_url,created_at,updated_at)
      values ($1,$2,$3,$4,$5,$6,coalesce($7,now()),coalesce($8,now()))
      on conflict (id) do update set internship_commitment_term_id=excluded.internship_commitment_term_id,updated_at=now()`,
        [
          id,
          id,
          termId,
          companyId,
          studentId,
          null,
          row.createdAt,
          row.updatedAt,
        ],
      );
      await mapPut("signed_internship_commitment_term", row.id, id);
      return true;
    },
  },
];

async function runJob(job: Job, ctx: Ctx) {
  if (ctx.tableFilter && ctx.tableFilter !== job.key) return;
  const runId = await startRun(job.key);
  try {
    const [rows] = await source.query(job.extractSql(ctx.mode));
    let lidos = 0;
    let inseridos = 0;
    for (const row of rows as any[]) {
      try {
        const ok = await job.upsert(row, ctx);
        if (ok) {
          inseridos++;
        } else {
          // IMPORTANT: do not silently drop rows (usually missing FK due to legacy INT/UUID mismatch)
          await target.query(
            `insert into migration_errors(run_id,job_name,legacy_id,stage,error_message,payload)
             values ($1,$2,$3,'validate',$4,$5)`,
            [
              runId,
              job.key,
              String(row[job.pk] ?? ""),
              "skipped: upsert returned false (missing FK or mapping)",
              row,
            ],
          );
        }
      } catch (e: any) {
        await target.query(
          `insert into migration_errors(run_id,job_name,legacy_id,stage,error_message,payload) values ($1,$2,$3,'load',$4,$5)`,
          [runId, job.key, String(row[job.pk] ?? ""), e.message, row],
        );
      }
      lidos++;
      if (lidos % batchSize === 0) console.log(`[${job.key}] ${lidos} rows`);
    }
    await endRun(runId, "success");
    console.log(`✅ ${job.key}: ${lidos} lidos / ${inseridos} inseridos`);
  } catch (e: any) {
    await endRun(runId, "failed", e.message);
    console.error(`❌ ${job.key}:`, e.message);
  }
}

async function main() {
  await ensureControlTables();
  const stateByUf = await bootstrapStates();
  const ctx: Ctx = { source, target, mode, tableFilter, stateByUf };
  for (const job of jobs.sort((a, b) => a.level - b.level))
    await runJob(job, ctx);
  await source.end();
  await target.end();
}

main().catch(async (e) => {
  console.error(e);
  await source.end();
  await target.end();
  process.exit(1);
});
