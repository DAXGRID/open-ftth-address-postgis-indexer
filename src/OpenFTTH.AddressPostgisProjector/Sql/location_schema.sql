CREATE SCHEMA IF NOT EXISTS "location"
    AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS location.official_access_address_bulk (
	id uuid PRIMARY KEY,
	coord public.geometry(point, 25832) NULL,
	status varchar(50) NULL,
	house_number varchar(50) NULL,
	road_code varchar(50) NULL,
	road_name varchar(255) NULL,
	town_name varchar(255) NULL,
	post_district_code varchar(50) NULL,
	post_district_name varchar(255) NULL,
	municipal_code varchar(50) NULL,
	access_address_external_id varchar(255) NULL,
	road_external_id varchar(255) NULL,
	plot_external_id varchar(255) NULL,
	deleted bool NOT NULL DEFAULT false);

-- Create view
CREATE MATERIALIZED VIEW IF NOT EXISTS location.official_access_address
AS
SELECT * FROM location.official_access_address_bulk;

CREATE UNIQUE INDEX IF NOT EXISTS ix_official_access_address_id ON location.official_access_address (id);

CREATE INDEX IF NOT EXISTS ix_official_access_address_access_address_external_id ON location.official_access_address USING btree (access_address_external_id);

CREATE INDEX IF NOT EXISTS ix_official_access_address_coord ON location.official_access_address USING gist (coord);

-- CREATE TABLE IF NOT EXISTS location.official_unit_address (
-- 	id uuid PRIMARY KEY,
-- 	access_address_id uuid NULL,
-- 	status varchar(50) NULL,
-- 	floor_name varchar(80) NULL,
-- 	suit_name varchar(80) NULL,
-- 	unit_address_external_id varchar(255) NULL,
-- 	access_address_external_id varchar(255) NULL,
-- 	deleted bool NOT NULL DEFAULT false);

-- CREATE INDEX IF NOT EXISTS  "IX_official_unit_address_access_address_external_id" ON location.official_unit_address USING btree (access_address_external_id);

-- CREATE INDEX IF NOT EXISTS  "IX_official_unit_address_access_address_id" ON location.official_unit_address USING btree (access_address_id);

-- CREATE UNIQUE INDEX IF NOT EXISTS  "IX_official_unit_address_unit_address_external_id" ON location.official_unit_address USING btree (unit_address_external_id);
