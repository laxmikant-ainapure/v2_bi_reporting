-- Drop tables if exits

-- DROP TABLE public."user";
CREATE TABLE IF NOT EXISTS public."user" (id bigint not null,	email varchar(255) NULL,	created_dt timestamptz NULL,    last_job_date timestamp without time zone default now()
);

-- Drop table statement

-- DROP TABLE public.repository;

CREATE TABLE IF NOT EXISTS public.repository (
    id bigint not null,
	organization_id int4 NULL,
	uid varchar(50) NULL,
	"name" varchar(255) NULL,
	has_snapshot bool NULL DEFAULT false,
	created_dt timestamptz NULL,
	updated_dt timestamptz NULL,
	is_deleted bool NULL DEFAULT false,
	repo_meta text NULL,
	"type" varchar NULL,
    last_job_date timestamp without time zone default now()
    );

-- Drop table

-- DROP TABLE public.organization;

CREATE TABLE IF NOT EXISTS public.organization (
	id bigint NOT NULL,
	uid varchar(50) NULL,
	"name" text NULL,
	slug text NULL,
	"type" varchar(255) NULL DEFAULT 'personal'::character varying,
	status varchar(50) NULL DEFAULT ''::character varying,
	deleted_dt timestamptz NULL  ,
    tenant_id int null,
	package_name varchar null,
	license_created_date timestamp without time zone,
	license_expiry_date timestamp without time zone,
    last_job_date timestamp without time zone default now()  
    );

-- Drop table

-- DROP TABLE public.organization_user;

CREATE TABLE IF NOT EXISTS public.organization_user (
	id bigint NOT NULL,
	organization_id int4 NULL,
	user_integration_id int4 NULL,
	status varchar(50) NULL DEFAULT ''::character varying,
	created_dt timestamptz NULL,
	role_name varchar,
	updated_dt timestamptz NULL,
    last_job_date timestamp without time zone default now()
    );

-- Drop table

-- DROP TABLE public.user_integration;

CREATE TABLE IF NOT EXISTS public.user_integration (
	id bigint NOT NULL,
	user_id int4 NULL,
	master_integration_id int4 NULL,		
	status varchar(50) NULL DEFAULT ''::character varying,
	created_dt timestamptz NULL,
	updated_dt timestamptz NULL,
	last_login_dt timestamptz NULL,
	first_login_dt timestamptz null,
    last_job_date timestamp without time zone default now()
);

-- Drop table

-- DROP TABLE public.repository_scan_history;

CREATE TABLE IF NOT EXISTS public.repository_scan_history (
	id bigint NOT NULL,
	scan_id varchar(50) NULL,
	repository_id int4 NULL,
	started_on timestamptz NULL,
	ended_on timestamptz NULL,
	current_step varchar NULL,
	status varchar(100) NULL,
	is_completed bool NULL,
	last_job_date timestamp without time zone default now()
);


-- Drop table

-- DROP TABLE public.repository_language;

CREATE TABLE IF NOT EXISTS public.repository_language (
	id bigint NOT NULL,
	repository_id int4 NULL,
	language_id int4 NULL,
    last_job_date timestamp without time zone default now()
);


-- Drop table

-- DROP TABLE public."language";

CREATE TABLE IF NOT EXISTS public."language" (
	id bigint NOT NULL,
	"name" varchar NULL,
	last_job_date timestamp without time zone default now()
);


-- Drop table

-- DROP TABLE public.license_metrics;

CREATE TABLE IF NOT EXISTS public.license_metrics (
	id bigint NOT NULL,
	tenant_uid text NULL,
	metric text NULL,
	value float4 NULL,
	ident text NULL,
	last_job_date timestamp without time zone default now()
);

-- Drop table

-- DROP TABLE public.review_request_queue;

CREATE TABLE IF NOT EXISTS public.review_request_queue (
	id bigint NOT NULL,
	review_request_id int4 NULL,
	repository_uid varchar(50) NULL,
	status varchar(50) NULL,
	updated_on timestamptz NULL,
	created_on timestamptz NULL,
	last_job_date timestamp without time zone default now()
);

-- Drop table

-- DROP TABLE public.review_requests;

CREATE TABLE IF NOT EXISTS public.review_request (
	id bigint NOT NULL,
	review_request_id varchar(255) NOT NULL,
	listened_on timestamptz NULL,
	repository_uid varchar(50) ,
	last_job_date timestamp without time zone default now()
	);


-- Drop table

-- DROP TABLE public.usage_history;

CREATE TABLE IF NOT EXISTS public.usage_history (
	id serial NOT NULL,
	repository_uid varchar(50) NOT NULL,
	tenant_uid varchar(50) NOT NULL,
	snapshot_id int4 NOT NULL,
	snapshot_time timestamptz NOT NULL,
	loc int8 NULL,
	last_job_date timestamp without time zone default now()
);


create table if not exists public.deleted_account_details
(
id serial NOT NULL,
event_type varchar,
user_name varchar,
customer_id varchar,
email varchar,
churn_info text,
account_created_dt date,
account_deleted_dt date,
last_job_date timestamp without time zone default now()
);

CREATE TABLE public.tenant (
	tenant_id int8 NOT NULL,
	uid varchar(50) NULL,
	created_dt timestamptz NULL DEFAULT now(),
	updated_dt timestamptz NULL,
	subdomain varchar NULL,
	is_trial bool NULL DEFAULT false,
	"type" varchar(150) NULL DEFAULT 'onpremise'::character varying,
	last_job_date timestamp without time zone default now()
);



