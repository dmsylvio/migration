import { pgTable, uniqueIndex, pgEnum, text, varchar, timestamp, integer, customType, date, boolean, numeric, index, pgSchema, serial, bigint, bigserial, jsonb } from "drizzle-orm/pg-core"
import { sql } from "drizzle-orm"

import { relations } from "drizzle-orm/relations";

export const drizzle = pgSchema("drizzle");
export const migration = pgSchema("migration");

export const userRole = pgEnum("user_role", ['student', 'company', 'institution', 'admin']);
export const internRequestStatus = pgEnum("intern_request_status", ['open', 'in_progress', 'filled', 'canceled', 'closed']);
export const internshipAgreementRequestStatus = pgEnum("internship_agreement_request_status", ['pending', 'accepted', 'rejected']);
export const internshipTerminationReason = pgEnum("internship_termination_reason", ['end_of_term', 'student_initiative', 'company_initiative', 'no_show_did_not_start', 'course_completion_or_dropout', 'hired_by_company', 'breach_of_contract', 'excessive_absences', 'other']);
export const internshipTerminationRequestStatus = pgEnum("internship_termination_request_status", ['pending', 'approved', 'cancelled']);
export const languageLevel = pgEnum("language_level", ['beginner', 'intermediate', 'advanced', 'native']);
export const opportunityStatus = pgEnum("opportunity_status", ['internal', 'published', 'closed', 'cancelled', 'archived']);
export const paymentFrequency = pgEnum("payment_frequency", ['hourly', 'daily', 'weekly', 'biweekly', 'monthly']);

export const company = pgTable("company", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	userId: text("user_id").notNull(),
	notes: text("notes"),
	legalName: text("legal_name").notNull(),
	tradeName: text("trade_name"),
	activities: text("activities"),
	cnpjNumber: varchar("cnpj_number", { length: 18 }).notNull(),
	stateRegistration: text("state_registration"),
	address: text("address"),
	city: text("city"),
	stateId: text("state_id").notNull(),
	zipCode: varchar("zip_code", { length: 9 }),
	phone: varchar("phone", { length: 20 }).notNull(),
	whatsapp: varchar("whatsapp", { length: 20 }),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("company_pkey").on(table.id),
	}
});

export const companyRepresentative = pgTable("company_representative", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	companyId: text("company_id").notNull(),
	fullName: text("full_name").notNull(),
	cpfNumber: varchar("cpf_number", { length: 11 }),
	rgNumber: varchar("rg_number", { length: 20 }),
	issuingAuthority: text("issuing_authority"),
	phone: varchar("phone", { length: 20 }),
	whatsapp: varchar("whatsapp", { length: 20 }),
	position: text("position"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("company_representative_pkey").on(table.id),
	}
});

export const companySupervisor = pgTable("company_supervisor", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	companyId: text("company_id").notNull(),
	fullName: text("full_name").notNull(),
	cpfNumber: varchar("cpf_number", { length: 11 }),
	rgNumber: varchar("rg_number", { length: 20 }),
	issuingAuthority: text("issuing_authority"),
	phone: varchar("phone", { length: 20 }),
	whatsapp: varchar("whatsapp", { length: 20 }),
	position: text("position"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("company_supervisor_pkey").on(table.id),
	}
});

export const course = pgTable("course", {
	id: text("id").primaryKey().notNull(),
	name: text("name").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("course_pkey").on(table.id),
	}
});

export const educationLevel = pgTable("education_level", {
	id: text("id").primaryKey().notNull(),
	name: text("name").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("education_level_pkey").on(table.id),
	}
});

export const educationalInstitution = pgTable("educational_institution", {
	id: text("id").primaryKey().notNull(),
	name: text("name").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("educational_institution_pkey").on(table.id),
	}
});

export const gender = pgTable("gender", {
	id: text("id").primaryKey().notNull(),
	name: text("name").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("gender_pkey").on(table.id),
	}
});

export const institutionRepresentative = pgTable("institution_representative", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	institutionId: text("institution_id").notNull(),
	fullName: text("full_name").notNull(),
	cpfNumber: varchar("cpf_number", { length: 11 }),
	rgNumber: varchar("rg_number", { length: 20 }),
	issuingAuthority: text("issuing_authority"),
	phone: varchar("phone", { length: 20 }),
	whatsapp: varchar("whatsapp", { length: 20 }),
	position: text("position"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("institution_representative_pkey").on(table.id),
	}
});

export const institutionSupervisor = pgTable("institution_supervisor", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	institutionId: text("institution_id").notNull(),
	fullName: text("full_name").notNull(),
	cpfNumber: varchar("cpf_number", { length: 11 }),
	rgNumber: varchar("rg_number", { length: 20 }),
	issuingAuthority: text("issuing_authority"),
	phone: varchar("phone", { length: 20 }),
	whatsapp: varchar("whatsapp", { length: 20 }),
	position: text("position"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("institution_supervisor_pkey").on(table.id),
	}
});

export const institutions = pgTable("institutions", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	userId: text("user_id").notNull(),
	notes: text("notes"),
	legalName: text("legal_name").notNull(),
	tradeName: text("trade_name"),
	activities: text("activities"),
	cnpjNumber: varchar("cnpj_number", { length: 18 }).notNull(),
	stateRegistration: text("state_registration"),
	address: text("address"),
	city: text("city"),
	stateId: text("state_id").notNull(),
	zipCode: varchar("zip_code", { length: 9 }),
	phone: varchar("phone", { length: 20 }).notNull(),
	whatsapp: varchar("whatsapp", { length: 20 }),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("institutions_pkey").on(table.id),
	}
});

export const internRequests = pgTable("intern_requests", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	companyId: text("company_id").notNull(),
	city: text("city").notNull(),
	phone: varchar("phone", { length: 20 }).notNull(),
	email: text("email").notNull(),
	interviewerName: text("interviewer_name").notNull(),
	interviewAvailability: text("interview_availability").default(sql`'To be scheduled via email/phone/whatssap'`).notNull(),
	courseId: text("course_id").notNull(),
	semesterId: text("semester_id").notNull(),
	numberOfOpenPositions: integer("number_of_open_positions").default(sql`1`).notNull(),
	preferredGenderId: text("preferred_gender_id"),
	internshipSchedule: text("internship_schedule").notNull(),
	stipendAmount: text("stipend_amount"),
	transportationAllowanceAmount: text("transportation_allowance_amount"),
	mealAllowanceAmount: text("meal_allowance_amount"),
	otherBenefits: text("other_benefits"),
	requiredSkills: text("required_skills"),
	activitiesDescription: text("activities_description"),
	status: internRequestStatus("status").default(sql`'open'`).notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("intern_requests_pkey").on(table.id),
	}
});

export const internshipAgreementRequests = pgTable("internship_agreement_requests", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	companyId: text("company_id").notNull(),
	companyRepresentativeId: text("company_representative_id").notNull(),
	companySupervisorId: text("company_supervisor_id").notNull(),
	companyPhone: varchar("company_phone", { length: 20 }).notNull(),
	companyEmail: text("company_email").notNull(),
	supervisorEmail: text("supervisor_email"),
	councilNumber: text("council_number"),
	studentName: text("student_name").notNull(),
	studentPhone: varchar("student_phone", { length: 20 }),
	courseId: text("course_id").notNull(),
	semesterId: text("semester_id").notNull(),
	proposedStartDate: date("proposed_start_date").notNull(),
	proposedEndDate: date("proposed_end_date").notNull(),
	weeklySchedule: text("weekly_schedule"),
	stipendAmount: text("stipend_amount"),
	transportationAllowanceAmount: text("transportation_allowance_amount"),
	otherBenefits: text("other_benefits"),
	activitiesDescription: text("activities_description").notNull(),
	requestDate: date("request_date").defaultNow().notNull(),
	status: internshipAgreementRequestStatus("status").default(sql`'pending'`).notNull(),
	notes: text("notes"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("internship_agreement_requests_pkey").on(table.id),
	}
});

export const internshipCommitmentTerm = pgTable("internship_commitment_term", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	publicId: text("public_id"),
	notes: text("notes"),
	companyId: text("company_id").notNull(),
	companySupervisorId: text("company_supervisor_id").notNull(),
	companySupervisorPosition: text("company_supervisor_position"),
	companyRepresentativeId: text("company_representative_id").notNull(),
	companyRepresentativePosition: text("company_representative_position"),
	institutionId: text("institution_id").notNull(),
	institutionSupervisorId: text("institution_supervisor_id").notNull(),
	institutionSupervisorPosition: text("institution_supervisor_position"),
	institutionRepresentativeId: text("institution_representative_id").notNull(),
	institutionRepresentativePosition: text("institution_representative_position"),
	studentId: text("student_id").notNull(),
	firstActivity: text("first_activity"),
	secondActivity: text("second_activity"),
	startCommitmentDate: date("start_commitment_date"),
	endCommitmentDate: date("end_commitment_date"),
	daysAndHoursPerWeek: text("days_and_hours_per_week"),
	stipendAmount: text("stipend_amount"),
	paymentFrequency: paymentFrequency("payment_frequency").default(sql`'monthly'`).notNull(),
	transportationAllowanceAmount: text("transportation_allowance_amount"),
	termDate: date("term_date").notNull(),
	firstExtensionDate: date("first_extension_date"),
	secondExtensionDate: date("second_extension_date"),
	thirdExtensionDate: date("third_extension_date"),
	terminationDate: date("termination_date"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("internship_commitment_term_pkey").on(table.id),
		publicIdUnique: uniqueIndex("internship_commitment_term_public_id_unique").on(table.publicId),
	}
});

export const internshipTerminationRequests = pgTable("internship_termination_requests", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	companyId: text("company_id").notNull(),
	internshipCommitmentTermId: text("internship_commitment_term_id").notNull(),
	dateOfTermination: date("date_of_termination").notNull(),
	reasonForTermination: internshipTerminationReason("reason_for_termination").notNull(),
	status: internshipTerminationRequestStatus("status").default(sql`'pending'`).notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("internship_termination_requests_pkey").on(table.id),
	}
});

export const maritalStatus = pgTable("marital_status", {
	id: text("id").primaryKey().notNull(),
	name: text("name").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("marital_status_pkey").on(table.id),
	}
});

export const opportunity = pgTable("opportunity", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	publicId: text("public_id"),
	companyId: text("company_id").notNull(),
	companySupervisorId: text("company_supervisor_id").notNull(),
	status: opportunityStatus("status").default(sql`'internal'`).notNull(),
	courseId: text("course_id").notNull(),
	openings: integer("openings").default(sql`1`).notNull(),
	contactName: text("contact_name").notNull(),
	referencePoint: text("reference_point").notNull(),
	scheduleText: text("schedule_text").notNull(),
	interviewerName: text("interviewer_name").notNull(),
	educationLevel: text("education_level").notNull(),
	semesterId: text("semester_id").notNull(),
	genderId: text("gender_id").notNull(),
	acceptsDisabledCandidates: boolean("accepts_disabled_candidates").default(sql`false`).notNull(),
	stipendAmount: numeric("stipend_amount", { precision: 10, scale: 2 }),
	benefitsText: text("benefits_text"),
	requirementsText: text("requirements_text"),
	activitiesText: text("activities_text"),
	applicationInstructions: text("application_instructions"),
	notes: text("notes"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("opportunity_pkey").on(table.id),
		publicIdUnique: uniqueIndex("opportunity_public_id_unique").on(table.publicId),
	}
});

export const passwordResetTokens = pgTable("password_reset_tokens", {
	id: text("id").primaryKey().notNull(),
	userId: text("user_id").notNull(),
	token: text("token").notNull(),
	expiresAt: timestamp("expires_at", { withTimezone: true, mode: 'string' }).notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("password_reset_tokens_pkey").on(table.id),
		tokenUnique: uniqueIndex("password_reset_tokens_token_unique").on(table.token),
		userIdIdx: index("password_reset_tokens_user_id_idx").on(table.userId),
	}
});

export const refreshTokens = pgTable("refresh_tokens", {
	id: text("id").primaryKey().notNull(),
	userId: text("user_id").notNull(),
	tokenHash: text("token_hash").notNull(),
	expiresAt: timestamp("expires_at", { withTimezone: true, mode: 'string' }).notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	revokedAt: timestamp("revoked_at", { withTimezone: true, mode: 'string' }),
	replacedByTokenId: text("replaced_by_token_id"),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("refresh_tokens_pkey").on(table.id),
		tokenHashUnique: uniqueIndex("refresh_tokens_token_hash_unique").on(table.tokenHash),
	}
});

export const semester = pgTable("semester", {
	id: text("id").primaryKey().notNull(),
	name: text("name").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("semester_pkey").on(table.id),
	}
});

export const shift = pgTable("shift", {
	id: text("id").primaryKey().notNull(),
	name: text("name").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("shift_pkey").on(table.id),
	}
});

export const signedInternshipCommitmentTerm = pgTable("signed_internship_commitment_term", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	publicId: text("public_id"),
	internshipCommitmentTermId: text("internship_commitment_term_id").notNull(),
	companyId: text("company_id").notNull(),
	studentId: text("student_id").notNull(),
	pdfUrl: text("pdf_url").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("signed_internship_commitment_term_pkey").on(table.id),
		publicIdUnique: uniqueIndex("signed_internship_commitment_term_public_id_unique").on(table.publicId),
	}
});

export const state = pgTable("state", {
	id: text("id").primaryKey().notNull(),
	name: text("name").notNull(),
	acronym: varchar("acronym", { length: 2 }).notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		pkey: uniqueIndex("state_pkey").on(table.id),
	}
});

export const student = pgTable("student", {
	id: text("id").primaryKey().notNull(),
	oldId: text("old_id"),
	userId: text("user_id").notNull(),
	notes: text("notes"),
	fullName: text("full_name").notNull(),
	birthDate: date("birth_date").notNull(),
	cpfNumber: varchar("cpf_number", { length: 14 }).notNull(),
	rgNumber: varchar("rg_number", { length: 20 }),
	issueAgency: text("issue_agency"),
	hasDriverLicense: boolean("has_driver_license").default(sql`false`).notNull(),
	genderId: text("gender_id").notNull(),
	maritalStatusId: text("marital_status_id").notNull(),
	hasDisability: boolean("has_disability").default(sql`false`).notNull(),
	disabilityType: text("disability_type"),
	fatherName: text("father_name"),
	motherName: text("mother_name"),
	address: text("address"),
	city: text("city"),
	stateId: text("state_id").notNull(),
	zipCode: varchar("zip_code", { length: 9 }),
	phone: varchar("phone", { length: 20 }),
	whatsapp: varchar("whatsapp", { length: 20 }),
	educationLevelId: text("education_level_id").notNull(),
	courseId: text("course_id").notNull(),
	educationalInstitutionId: text("educational_institution_id").notNull(),
	hasOabLicense: boolean("has_oab_license").default(sql`false`).notNull(),
	enrollment: text("enrollment"),
	semesterId: text("semester_id").notNull(),
	shiftId: text("shift_id").notNull(),
	availableShiftId: text("available_shift_id").notNull(),
	englishLevel: languageLevel("english_level").default(sql`'beginner'`).notNull(),
	spanishLevel: languageLevel("spanish_level").default(sql`'beginner'`).notNull(),
	frenchLevel: languageLevel("french_level").default(sql`'beginner'`).notNull(),
	otherLanguages: text("other_languages"),
	improvementCourses: text("improvement_courses"),
	itCourses: text("it_courses"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("student_pkey").on(table.id),
	}
});

export const users = pgTable("users", {
	id: text("id").primaryKey().notNull(),
	email: text("email").notNull(),
	name: text("name"),
	avatar: text("avatar"),
	password: text("password").notNull(),
	role: userRole("role").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).notNull(),
	oldId: text("old_id"),
},
(table) => {
	return {
		emailUnique: uniqueIndex("users_email_unique").on(table.email),
		pkey: uniqueIndex("users_pkey").on(table.id),
	}
});

export const __drizzleMigrationsInDrizzle = drizzle.table("__drizzle_migrations", {
	id: serial("id").primaryKey().notNull(),
	hash: text("hash").notNull(),
	createdAt: bigint("created_at", { mode: "number" }),
},
(table) => {
	return {
		pkey: uniqueIndex("__drizzle_migrations_pkey").on(table.id),
	}
});

export const migrationCheckpointsInMigration = migration.table("migration_checkpoints", {
	entity: text("entity").primaryKey().notNull(),
	checkpointType: text("checkpoint_type").notNull(),
	checkpointValue: text("checkpoint_value").notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("migration_checkpoints_pkey").on(table.entity),
	}
});

export const migrationErrorsInMigration = migration.table("migration_errors", {
	id: bigserial("id", { mode: "bigint" }).primaryKey().notNull(),
	runId: bigint("run_id", { mode: "number" }),
	entity: text("entity").notNull(),
	sourcePk: text("source_pk"),
	error: text("error").notNull(),
	payload: jsonb("payload"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
},
(table) => {
	return {
		pkey: uniqueIndex("migration_errors_pkey").on(table.id),
	}
});

export const migrationRunsInMigration = migration.table("migration_runs", {
	id: bigserial("id", { mode: "bigint" }).primaryKey().notNull(),
	mode: text("mode").notNull(),
	startedAt: timestamp("started_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	finishedAt: timestamp("finished_at", { withTimezone: true, mode: 'string' }),
	status: text("status").default(sql`'running'`).notNull(),
	notes: text("notes"),
},
(table) => {
	return {
		pkey: uniqueIndex("migration_runs_pkey").on(table.id),
	}
});export const companyRelations = relations(company, ({one, many}) => ({
	state: one(state, {
		fields: [company.stateId],
		references: [state.id]
	}),
	user: one(users, {
		fields: [company.userId],
		references: [users.id]
	}),
	companyRepresentatives: many(companyRepresentative),
	companySupervisors: many(companySupervisor),
	internRequests: many(internRequests),
	internshipAgreementRequests: many(internshipAgreementRequests),
	internshipCommitmentTerms: many(internshipCommitmentTerm),
	internshipTerminationRequests: many(internshipTerminationRequests),
	opportunities: many(opportunity),
	signedInternshipCommitmentTerms: many(signedInternshipCommitmentTerm),
}));

export const stateRelations = relations(state, ({many}) => ({
	companies: many(company),
	institutions: many(institutions),
	students: many(student),
}));

export const usersRelations = relations(users, ({many}) => ({
	companies: many(company),
	institutions: many(institutions),
	passwordResetTokens: many(passwordResetTokens),
	refreshTokens: many(refreshTokens),
	students: many(student),
}));

export const companyRepresentativeRelations = relations(companyRepresentative, ({one, many}) => ({
	company: one(company, {
		fields: [companyRepresentative.companyId],
		references: [company.id]
	}),
	internshipAgreementRequests: many(internshipAgreementRequests),
	internshipCommitmentTerms: many(internshipCommitmentTerm),
}));

export const companySupervisorRelations = relations(companySupervisor, ({one, many}) => ({
	company: one(company, {
		fields: [companySupervisor.companyId],
		references: [company.id]
	}),
	internshipAgreementRequests: many(internshipAgreementRequests),
	internshipCommitmentTerms: many(internshipCommitmentTerm),
	opportunities: many(opportunity),
}));

export const institutionRepresentativeRelations = relations(institutionRepresentative, ({one, many}) => ({
	institution: one(institutions, {
		fields: [institutionRepresentative.institutionId],
		references: [institutions.id]
	}),
	internshipCommitmentTerms: many(internshipCommitmentTerm),
}));

export const institutionsRelations = relations(institutions, ({one, many}) => ({
	institutionRepresentatives: many(institutionRepresentative),
	institutionSupervisors: many(institutionSupervisor),
	state: one(state, {
		fields: [institutions.stateId],
		references: [state.id]
	}),
	user: one(users, {
		fields: [institutions.userId],
		references: [users.id]
	}),
	internshipCommitmentTerms: many(internshipCommitmentTerm),
}));

export const institutionSupervisorRelations = relations(institutionSupervisor, ({one, many}) => ({
	institution: one(institutions, {
		fields: [institutionSupervisor.institutionId],
		references: [institutions.id]
	}),
	internshipCommitmentTerms: many(internshipCommitmentTerm),
}));

export const internRequestsRelations = relations(internRequests, ({one}) => ({
	company: one(company, {
		fields: [internRequests.companyId],
		references: [company.id]
	}),
	course: one(course, {
		fields: [internRequests.courseId],
		references: [course.id]
	}),
	gender: one(gender, {
		fields: [internRequests.preferredGenderId],
		references: [gender.id]
	}),
	semester: one(semester, {
		fields: [internRequests.semesterId],
		references: [semester.id]
	}),
}));

export const courseRelations = relations(course, ({many}) => ({
	internRequests: many(internRequests),
	internshipAgreementRequests: many(internshipAgreementRequests),
	opportunities: many(opportunity),
	students: many(student),
}));

export const genderRelations = relations(gender, ({many}) => ({
	internRequests: many(internRequests),
	opportunities: many(opportunity),
	students: many(student),
}));

export const semesterRelations = relations(semester, ({many}) => ({
	internRequests: many(internRequests),
	internshipAgreementRequests: many(internshipAgreementRequests),
	opportunities: many(opportunity),
	students: many(student),
}));

export const internshipAgreementRequestsRelations = relations(internshipAgreementRequests, ({one}) => ({
	company: one(company, {
		fields: [internshipAgreementRequests.companyId],
		references: [company.id]
	}),
	companyRepresentative: one(companyRepresentative, {
		fields: [internshipAgreementRequests.companyRepresentativeId],
		references: [companyRepresentative.id]
	}),
	companySupervisor: one(companySupervisor, {
		fields: [internshipAgreementRequests.companySupervisorId],
		references: [companySupervisor.id]
	}),
	course: one(course, {
		fields: [internshipAgreementRequests.courseId],
		references: [course.id]
	}),
	semester: one(semester, {
		fields: [internshipAgreementRequests.semesterId],
		references: [semester.id]
	}),
}));

export const internshipCommitmentTermRelations = relations(internshipCommitmentTerm, ({one, many}) => ({
	company: one(company, {
		fields: [internshipCommitmentTerm.companyId],
		references: [company.id]
	}),
	companyRepresentative: one(companyRepresentative, {
		fields: [internshipCommitmentTerm.companyRepresentativeId],
		references: [companyRepresentative.id]
	}),
	companySupervisor: one(companySupervisor, {
		fields: [internshipCommitmentTerm.companySupervisorId],
		references: [companySupervisor.id]
	}),
	institution: one(institutions, {
		fields: [internshipCommitmentTerm.institutionId],
		references: [institutions.id]
	}),
	institutionRepresentative: one(institutionRepresentative, {
		fields: [internshipCommitmentTerm.institutionRepresentativeId],
		references: [institutionRepresentative.id]
	}),
	institutionSupervisor: one(institutionSupervisor, {
		fields: [internshipCommitmentTerm.institutionSupervisorId],
		references: [institutionSupervisor.id]
	}),
	student: one(student, {
		fields: [internshipCommitmentTerm.studentId],
		references: [student.id]
	}),
	internshipTerminationRequests: many(internshipTerminationRequests),
	signedInternshipCommitmentTerms: many(signedInternshipCommitmentTerm),
}));

export const studentRelations = relations(student, ({one, many}) => ({
	internshipCommitmentTerms: many(internshipCommitmentTerm),
	signedInternshipCommitmentTerms: many(signedInternshipCommitmentTerm),
	shift_availableShiftId: one(shift, {
		fields: [student.availableShiftId],
		references: [shift.id],
		relationName: "student_availableShiftId_shift_id"
	}),
	course: one(course, {
		fields: [student.courseId],
		references: [course.id]
	}),
	educationLevel: one(educationLevel, {
		fields: [student.educationLevelId],
		references: [educationLevel.id]
	}),
	educationalInstitution: one(educationalInstitution, {
		fields: [student.educationalInstitutionId],
		references: [educationalInstitution.id]
	}),
	gender: one(gender, {
		fields: [student.genderId],
		references: [gender.id]
	}),
	maritalStatus: one(maritalStatus, {
		fields: [student.maritalStatusId],
		references: [maritalStatus.id]
	}),
	semester: one(semester, {
		fields: [student.semesterId],
		references: [semester.id]
	}),
	shift_shiftId: one(shift, {
		fields: [student.shiftId],
		references: [shift.id],
		relationName: "student_shiftId_shift_id"
	}),
	state: one(state, {
		fields: [student.stateId],
		references: [state.id]
	}),
	user: one(users, {
		fields: [student.userId],
		references: [users.id]
	}),
}));

export const internshipTerminationRequestsRelations = relations(internshipTerminationRequests, ({one}) => ({
	company: one(company, {
		fields: [internshipTerminationRequests.companyId],
		references: [company.id]
	}),
	internshipCommitmentTerm: one(internshipCommitmentTerm, {
		fields: [internshipTerminationRequests.internshipCommitmentTermId],
		references: [internshipCommitmentTerm.id]
	}),
}));

export const opportunityRelations = relations(opportunity, ({one}) => ({
	company: one(company, {
		fields: [opportunity.companyId],
		references: [company.id]
	}),
	companySupervisor: one(companySupervisor, {
		fields: [opportunity.companySupervisorId],
		references: [companySupervisor.id]
	}),
	course: one(course, {
		fields: [opportunity.courseId],
		references: [course.id]
	}),
	gender: one(gender, {
		fields: [opportunity.genderId],
		references: [gender.id]
	}),
	semester: one(semester, {
		fields: [opportunity.semesterId],
		references: [semester.id]
	}),
}));

export const passwordResetTokensRelations = relations(passwordResetTokens, ({one}) => ({
	user: one(users, {
		fields: [passwordResetTokens.userId],
		references: [users.id]
	}),
}));

export const refreshTokensRelations = relations(refreshTokens, ({one}) => ({
	user: one(users, {
		fields: [refreshTokens.userId],
		references: [users.id]
	}),
}));

export const signedInternshipCommitmentTermRelations = relations(signedInternshipCommitmentTerm, ({one}) => ({
	company: one(company, {
		fields: [signedInternshipCommitmentTerm.companyId],
		references: [company.id]
	}),
	internshipCommitmentTerm: one(internshipCommitmentTerm, {
		fields: [signedInternshipCommitmentTerm.internshipCommitmentTermId],
		references: [internshipCommitmentTerm.id]
	}),
	student: one(student, {
		fields: [signedInternshipCommitmentTerm.studentId],
		references: [student.id]
	}),
}));

export const shiftRelations = relations(shift, ({many}) => ({
	students_availableShiftId: many(student, {
		relationName: "student_availableShiftId_shift_id"
	}),
	students_shiftId: many(student, {
		relationName: "student_shiftId_shift_id"
	}),
}));

export const educationLevelRelations = relations(educationLevel, ({many}) => ({
	students: many(student),
}));

export const educationalInstitutionRelations = relations(educationalInstitution, ({many}) => ({
	students: many(student),
}));

export const maritalStatusRelations = relations(maritalStatus, ({many}) => ({
	students: many(student),
}));

export const migrationErrorsInMigrationRelations = relations(migrationErrorsInMigration, ({one}) => ({
	migrationRunsInMigration: one(migrationRunsInMigration, {
		fields: [migrationErrorsInMigration.runId],
		references: [migrationRunsInMigration.id]
	}),
}));

export const migrationRunsInMigrationRelations = relations(migrationRunsInMigration, ({many}) => ({
	migrationErrorsInMigrations: many(migrationErrorsInMigration),
}));