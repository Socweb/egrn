'''

CREATE TABLE stg.main_short_info(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    dfrom timestamptz NOT NULL,
    org_name text NOT NULL,
    status varchar(20) NOT NULL,
    org_type varchar(2) NOT NULL
);

CREATE TABLE stg.main_address_info(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    vregion varchar(20),
    vdistrict varchar(30),
    vnp varchar(100),
    vulitsa varchar(100),
    vdom text,
    vpom text,
    vemail text,
    vtels text,
    location text
);

CREATE TABLE stg.main_okved_info(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL,
    okved_code int NOT NULL,
    okved_text text NOT NULL
);
-- DDS
CREATE TABLE dds.ngrns(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ngrn int NOT NULL UNIQUE
);

CREATE TABLE dds.org_names(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_name text NOT NULL
);

CREATE TABLE dds.okveds(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    okved_code int NOT NULL UNIQUE,
    okved_text text NOT NULL UNIQUE
);

CREATE TABLE dds.statuses(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status varchar(20) NOT NULL UNIQUE
);

CREATE TABLE dds.org_types(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_type varchar(2) NOT NULL UNIQUE
);

CREATE TABLE dds.regions(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region varchar(20)
);

CREATE TABLE dds.districts(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    district varchar(30)
);

CREATE TABLE dds.settlements(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    settlement varchar(100)
);

CREATE TABLE dds.contacts(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location text,
    email text,
    phone text,
    UNIQUE(location, email, phone)
);

CREATE TABLE dds.dates(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date timestamptz NOT NULL UNIQUE
);

-- Загрузка в DDS

INSERT INTO dds.ngrns (ngrn)
SELECT distinct ON (ngrn) ngrn
FROM stg.main_short_info;

INSERT INTO dds.org_names (org_name)
SELECT distinct ON (org_name) org_name
FROM stg.main_short_info;

INSERT INTO dds.okveds (okved_code, okved_text)
SELECT distinct ON (okved_code, okved_text) okved_code, okved_text
FROM stg.main_okved_info;

INSERT INTO dds.statuses (status)
SELECT distinct ON (status) status
FROM stg.main_short_info;

INSERT INTO dds.org_types (org_type)
SELECT distinct ON (org_type) org_type
FROM stg.main_short_info;

INSERT INTO dds.regions (region)
SELECT distinct ON (vregion) vregion
FROM stg.main_address_info;

INSERT INTO dds.districts (district)
SELECT distinct ON (vdistrict) vdistrict
FROM stg.main_address_info;

INSERT INTO dds.settlements (settlement)
SELECT distinct ON (vnp) vnp
FROM stg.main_address_info;

INSERT INTO dds.contacts (location, email, phone)
SELECT distinct ON (location, vemail, vtels) location, vemail, vtels
FROM stg.main_address_info;

INSERT INTO dds.dates (date)
SELECT distinct ON (dfrom) dfrom
FROM stg.main_short_info;

-- DM
CREATE TABLE dm.organization(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_name text NOT NULL,
    ngrn int NOT NULL REFERENCES dds.ngrns (ngrn),
    org_type varchar(2) NOT NULL REFERENCES dds.org_types (org_type),
    date timestamptz NOT NULL  REFERENCES dds.dates (date),
    status varchar(20) NOT NULL REFERENCES dds.statuses (status),
    okved_code int NOT NULL REFERENCES dds.okveds (okved_code),
    okved_text text NOT NULL REFERENCES dds.okveds (okved_text),
    region varchar(20),
    district varchar(30),
    settlement text,
    location text,
    email text,
    phone text,
    UNIQUE(org_name, ngrn),
    UNIQUE(location, email, phone)
);

-- Загрузка в DM
INSERT INTO dm.organization(
    org_name, ngrn, org_type, date, status, okved_code, okved_text,
    region, district, settlement, location, email, phone
    )
SELECT DISTINCT ON (org_name, ngrn, status)
msi.org_name,
msi.ngrn,
msi.org_type,
msi.dfrom,
msi.status,
moi.okved_code,
moi.okved_text,
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
ON msi.ngrn = mai.ngrn;


ALTER TABLE stg.main_short_info OWNER TO egrn;
ALTER TABLE stg.main_address_info OWNER TO egrn;
ALTER TABLE stg.main_okved_info OWNER TO egrn;

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


'''
