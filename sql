-- Создание слоя сырых данных

DROP TABLE IF EXISTS stg.main_short_info;
CREATE TABLE stg.main_short_info(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    dfrom timestamptz NOT NULL,
    org_name text NOT NULL,
    status text NOT NULL,
    org_type varchar(2) NOT NULL
);
CREATE INDEX main_short_info_index ON stg.main_short_info(ngrn, dfrom, org_name, status, org_type);

DROP TABLE IF EXISTS stg.main_address_info;
CREATE TABLE stg.main_address_info(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    dfrom timestamptz NOT NULL,
    vregion varchar(20),
    vdistrict varchar(30),
    vnp text,
    vulitsa text,
    vdom text,
    vpom text,
    vemail text,
    vtels text,
    location text
);
CREATE INDEX main_address_info_index ON stg.main_address_info(ngrn, dfrom, vregion, vdistrict, vnp, vulitsa, vdom, vpom, vemail, location);

DROP TABLE IF EXISTS stg.main_okved_info;
CREATE TABLE stg.main_okved_info(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    dfrom timestamptz NOT NULL,
    okved_code varchar(10) NOT NULL,
    okved_text text NOT NULL
);

DROP TABLE IF EXISTS stg.short_info_tech_table;
CREATE TABLE stg.short_info_tech_table(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    max_dfrom timestamptz NOT NULL UNIQUE
);
-- Первичная запись таблицы
INSERT INTO stg.short_info_tech_table (max_dfrom) VALUES ('1991-01-01'::timestamptz);

DROP TABLE IF EXISTS stg.address_info_tech_table;
CREATE TABLE stg.address_info_tech_table(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    max_dfrom timestamptz NOT NULL UNIQUE
);
INSERT INTO stg.address_info_tech_table (max_dfrom) VALUES ('1991-01-01'::timestamptz);

DROP TABLE IF EXISTS stg.okved_info_tech_table;
CREATE TABLE stg.okved_info_tech_table(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    max_dfrom timestamptz NOT NULL UNIQUE
);
INSERT INTO stg.okved_info_tech_table (max_dfrom) VALUES ('1991-01-01'::timestamptz);

DROP TABLE IF EXISTS stg.tg_bot_tech_table;
CREATE TABLE stg.tg_bot_tech_table(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    count int NOT NULL UNIQUE
);

-- Удаление дубликатов из сырых данных
create table stg.cur_main_short_info(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    dfrom timestamptz NOT NULL,
    org_name text NOT NULL,
    status text NOT NULL,
    org_type varchar(2) NOT NULL
);

insert into stg.cur_main_short_info (ngrn, dfrom, org_name, status, org_type)
    select ngrn, dfrom, org_name, status, org_type
    from stg.main_short_info;

truncate table stg.main_short_info RESTART IDENTITY;
insert into stg.main_short_info (ngrn, dfrom, org_name, status, org_type)
    select ngrn, dfrom, org_name, status, org_type
    from stg.cur_main_short_info;

DROP TABLE IF EXISTS stg.cur_main_short_info;

create table stg.cur_main_address_info(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    dfrom timestamptz NOT NULL,
    vregion varchar(20),
    vdistrict varchar(30),
    vnp text,
    vulitsa text,
    vdom text,
    vpom text,
    vemail text,
    vtels text,
    location text
);

insert into stg.cur_main_address_info (ngrn, dfrom, vregion, vdistrict, vnp, vulitsa, vdom, vpom, vemail, vtels, location)
    select ngrn, dfrom, vregion, vdistrict, vnp, vulitsa, vdom, vpom, vemail, vtels, location
    from stg.main_address_info;

truncate table stg.main_address_info RESTART IDENTITY;
insert into stg.main_address_info (ngrn, dfrom, vregion, vdistrict, vnp, vulitsa, vdom, vpom, vemail, vtels, location)
    select ngrn, dfrom, vregion, vdistrict, vnp, vulitsa, vdom, vpom, vemail, vtels, location
    from stg.cur_main_address_info;

DROP TABLE IF EXISTS stg.cur_main_address_info;

create table stg.cur_main_okved_info (
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    dfrom timestamptz NOT NULL,
    okved_code varchar(10) NOT NULL,
    okved_text text NOT NULL
);

insert into stg.cur_main_okved_info (ngrn, dfrom, okved_code, okved_text)
    select ngrn, dfrom, okved_code, okved_text
    from stg.main_okved_info;

truncate table stg.main_okved_info RESTART IDENTITY;
insert into stg.main_okved_info (ngrn, dfrom, okved_code, okved_text)
    select ngrn, dfrom, okved_code, okved_text
    from stg.cur_main_okved_info;

DROP TABLE IF EXISTS stg.cur_main_okved_info;






DELETE FROM stg.main_short_info
WHERE id IN (SELECT id
              FROM (SELECT id,
                             ROW_NUMBER() OVER (partition BY ngrn, dfrom, org_name, status ORDER BY id) AS rnum
                     FROM stg.main_short_info) as t
              WHERE t.rnum > 1);

DELETE FROM stg.main_address_info
WHERE id IN (SELECT id
              FROM (SELECT id,
                             ROW_NUMBER() OVER (partition BY ngrn, location ORDER BY id) AS rnum
                     FROM stg.main_short_info) as t
              WHERE t.rnum > 1);

DELETE FROM stg.main_okved_info
WHERE id IN (SELECT id
              FROM (SELECT id,
                             ROW_NUMBER() OVER (partition BY okved_code, okved_text ORDER BY id) AS rnum
                     FROM stg.main_short_info) as t
              WHERE t.rnum > 1);

-- Заполнение тех. таблиц

INSERT INTO stg.short_info_tech_table (max_dfrom)
SELECT MAX(dfrom)
FROM stg.main_short_info
ON CONFLICT ON CONSTRAINT short_info_tech_table_max_dfrom_key
DO NOTHING;

INSERT INTO stg.address_info_tech_table (max_dfrom)
SELECT MAX(dfrom)
FROM stg.main_address_info
ON CONFLICT ON CONSTRAINT address_info_tech_table_max_dfrom_key
DO NOTHING;

INSERT INTO stg.okved_info_tech_table (max_dfrom)
SELECT MAX(dfrom)
FROM stg.main_okved_info
ON CONFLICT ON CONSTRAINT okved_info_tech_table_max_dfrom_key
DO NOTHING;

-- DDS
DROP TABLE IF EXISTS dds.ngrns CASCADE;
CREATE TABLE dds.ngrns(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dds.org_names CASCADE;
CREATE TABLE dds.org_names(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_name text NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dds.okveds CASCADE;
CREATE TABLE dds.okveds(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    okved_code varchar(10) NOT NULL,
    okved_text text NOT NULL,
    UNIQUE(okved_code, okved_text)
);

DROP TABLE IF EXISTS dds.statuses CASCADE;
CREATE TABLE dds.statuses(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status text NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dds.org_types CASCADE;
CREATE TABLE dds.org_types(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_type varchar(2) NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dds.regions CASCADE;
CREATE TABLE dds.regions(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region varchar(20) NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dds.districts CASCADE;
CREATE TABLE dds.districts(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    district varchar(30) NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dds.settlements CASCADE;
CREATE TABLE dds.settlements(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    settlement text NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dds.contacts CASCADE;
CREATE TABLE dds.contacts(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location text NOT NULL,
    email text NOT NULL,
    phone text NOT NULL,
    UNIQUE(location, email, phone)
);

DROP TABLE IF EXISTS dds.dates CASCADE;
CREATE TABLE dds.dates(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date timestamptz NOT NULL UNIQUE
);

-- Загрузка в DDS

INSERT INTO dds.ngrns (ngrn)
SELECT distinct ON (ngrn) ngrn
FROM stg.main_short_info as msi
WHERE msi.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT ngrns_ngrn_key
DO NOTHING;

INSERT INTO dds.org_names (org_name)
SELECT distinct ON (org_name) org_name
FROM stg.main_short_info as msi
WHERE msi.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT org_names_org_name_key
DO NOTHING;

INSERT INTO dds.okveds (okved_code, okved_text)
SELECT distinct ON (okved_code, okved_text) okved_code, okved_text
FROM stg.main_okved_info as moi
WHERE moi.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.okved_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT okveds_okved_code_okved_text_key
DO NOTHING;

INSERT INTO dds.statuses (status)
SELECT distinct ON (status) status
FROM stg.main_short_info as msi
WHERE msi.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT statuses_status_key
DO NOTHING;

INSERT INTO dds.org_types (org_type)
SELECT distinct ON (org_type) org_type
FROM stg.main_short_info as msi
WHERE msi.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT org_types_org_type_key
DO NOTHING;

INSERT INTO dds.regions (region)
SELECT distinct ON (vregion) vregion
FROM stg.main_address_info as mai
WHERE mai.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT regions_region_key
DO NOTHING;

INSERT INTO dds.districts (district)
SELECT distinct ON (vdistrict) vdistrict
FROM stg.main_address_info as mai
WHERE mai.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT districts_district_key
DO NOTHING;

INSERT INTO dds.settlements (settlement)
SELECT distinct ON (vnp) vnp
FROM stg.main_address_info as mai
WHERE mai.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT settlements_settlement_key
DO NOTHING;


INSERT INTO dds.contacts (location, email, phone)
SELECT distinct ON (location, vemail, vtels) location, vemail, vtels
FROM stg.main_address_info as mai
WHERE mai.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT contacts_location_email_phone_key
DO NOTHING;

INSERT INTO dds.dates (date)
SELECT distinct ON (dfrom) dfrom
FROM stg.main_short_info as msi 
WHERE msi.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT dates_date_key
DO NOTHING;

-- DM
DROP TABLE IF EXISTS dm.organization;
CREATE TABLE dm.organization(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_name text NOT NULL,
    ngrn int NOT NULL REFERENCES dds.ngrns (ngrn),
    org_type varchar(2) NOT NULL REFERENCES dds.org_types (org_type),
    date timestamptz NOT NULL  REFERENCES dds.dates (date),
    status text NOT NULL REFERENCES dds.statuses (status),
    okved_code varchar(10) NOT NULL,
    okved_text text NOT NULL,
    region varchar(20),
    district varchar(30),
    settlement text,
    location text,
    email text,
    phone text,
    UNIQUE(org_name, ngrn)
);
CREATE INDEX organization_index ON dm.organization(org_name, ngrn, org_type, date, status, okved_code, okved_text, region, district, settlement, location, email, phone);

-- Загрузка в DM
INSERT INTO dm.organization(
    org_name, ngrn, org_type, date, status, okved_code, okved_text,
    region, district, settlement, location, email, phone
    )
SELECT DISTINCT ON (org_name, ngrn, status, location)
msi.org_name,
msi.ngrn,
msi.org_type,
msi.dfrom,
msi.status,
COALESCE(moi.okved_code, 'Нет') as okved_code,
COALESCE(moi.okved_text, 'Нет') as okved_text,
mai.vregion,
mai.vdistrict,
mai.vnp,
mai.location,
mai.vemail,
mai.vtels
FROM stg.main_short_info as msi
LEFT JOIN
stg.main_okved_info as moi
ON msi.ngrn = moi.ngrn
LEFT JOIN
stg.main_address_info as mai
ON msi.ngrn = mai.ngrn
WHERE msi.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
    ) as t WHERE dfrom_rank = 2) or 
    mai.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
    ) as t WHERE dfrom_rank = 2) or
    moi.dfrom >= (SELECT max_dfrom FROM (
    SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.okved_info_tech_table
    ) as t WHERE dfrom_rank = 2)
ON CONFLICT ON CONSTRAINT organization_org_name_ngrn_key
DO UPDATE SET date = EXCLUDED.date, status = EXCLUDED.status, okved_code = EXCLUDED.okved_code, okved_text = EXCLUDED.okved_text;


ALTER TABLE stg.main_short_info OWNER TO egrn;
ALTER TABLE stg.main_address_info OWNER TO egrn;
ALTER TABLE stg.main_okved_info OWNER TO egrn;

ALTER TABLE stg.short_info_tech_table OWNER TO egrn;
ALTER TABLE stg.address_info_tech_table OWNER TO egrn;
ALTER TABLE stg.okved_info_tech_table OWNER TO egrn;
ALTER TABLE stg.tg_bot_tech_table OWNER TO egrn;

ALTER TABLE dds.ngrns OWNER TO egrn;
ALTER TABLE dds.org_names OWNER TO egrn;
ALTER TABLE dds.okveds OWNER TO egrn;
ALTER TABLE dds.statuses OWNER TO egrn;
ALTER TABLE dds.org_types OWNER TO egrn;
ALTER TABLE dds.regions OWNER TO egrn;
ALTER TABLE dds.districts OWNER TO egrn;
ALTER TABLE dds.settlements OWNER TO egrn;
ALTER TABLE dds.contacts OWNER TO egrn;
ALTER TABLE dds.dates OWNER TO egrn;

ALTER TABLE dm.organization OWNER TO egrn;



TRUNCATE TABLE stg.main_short_info;
TRUNCATE TABLE stg.main_address_info;
TRUNCATE TABLE stg.main_okved_info;

TRUNCATE TABLE stg.short_info_tech_table;
TRUNCATE TABLE stg.address_info_tech_table;
TRUNCATE TABLE stg.okved_info_tech_table;
TRUNCATE TABLE stg.tg_bot_tech_table;
INSERT INTO stg.short_info_tech_table (max_dfrom) VALUES ('1991-01-01'::timestamptz);
INSERT INTO stg.address_info_tech_table (max_dfrom) VALUES ('1991-01-01'::timestamptz);
INSERT INTO stg.okved_info_tech_table (max_dfrom) VALUES ('1991-01-01'::timestamptz);

TRUNCATE TABLE dds.ngrns  RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.org_names RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.okveds RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.statuses RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.org_types RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.regions RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.districts RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.settlements RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.contacts RESTART IDENTITY CASCADE;
TRUNCATE TABLE dds.dates RESTART IDENTITY CASCADE;

TRUNCATE TABLE dm.organization;
