CREATE DATABASE dbs_cohorts;

CREATE SCHEMA request AUTHORIZATION postgres;

CREATE TABLE request.request_landing (
	record_type varchar(255) NULL,
	local_pid varchar(255) NULL,
	date_of_birth varchar(255) NULL,
	date_of_death varchar(255) NULL,
	old_nhs_number varchar(255) NULL,
	new_nhs_number varchar(255) NULL,
	surname varchar(255) NULL,
	prev_alt_surname varchar(255) NULL,
	forename varchar(255) NULL,
	alternative_forename varchar(255) NULL,
	sex varchar(255) NULL,
	pcd_address_line1 varchar(255) NULL,
	pcd_address_line2 varchar(255) NULL,
	pcd_address_line3 varchar(255) NULL,
	pcd_address_line4 varchar(255) NULL,
	pcd_address_line5 varchar(255) NULL,
	pcd_postcode varchar(255) NULL,
	prev_address_line1 varchar(255) NULL,
	prev_address_line2 varchar(255) NULL,
	prev_address_line3 varchar(255) NULL,
	prev_address_line4 varchar(255) NULL,
	prev_address_line5 varchar(255) NULL,
	prev_postcode varchar(255) NULL,
	registered_gp varchar(255) NULL,
	registered_gp_practice varchar(255) NULL,
	prev_registered_gp_practice varchar(255) NULL,
	prev_registered_gp_office varchar(255) NULL
);

CREATE TABLE request.batch_request_tracker (
	file_sequence_number int4 NOT NULL,
	num_records int4 NULL,
	header_string varchar(100) NULL,
	footer_string varchar(100) NULL,
	date_generated timestamp NOT NULL DEFAULT now(),
	CONSTRAINT batch_request_tracker_pkey PRIMARY KEY (file_sequence_number)
);

