--------------------------------------------------------------------------------
--------------------------- STAGING AREA ---------------------------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS staging_world_happiness;
CREATE TABLE staging_world_happiness(
	country VARCHAR,
	region VARCHAR,
	happiness_rank INTEGER,
	happiness_score NUMERIC,
	happiness_score_lower NUMERIC,
	happiness_score_upper NUMERIC,
	economy_factor NUMERIC,
	family_factor NUMERIC,
	health_factor NUMERIC,
	freedom_factor NUMERIC,
	government_trust_factor NUMERIC,
	generosity_factor NUMERIC,
	dystopia_residual NUMERIC
);

DROP TABLE IF EXISTS staging_us_cities;
CREATE TABLE staging_us_cities (
    city VARCHAR,
    state VARCHAR,
    median_age NUMERIC,
    male_population INTEGER,
    female_population INTEGER,
    total_population INTEGER,
    number_of_veterans INTEGER,
    foreign_born INTEGER,
    average_household_size NUMERIC,
    state_code VARCHAR,
    race VARCHAR,
    race_count INTEGER
);

DROP TABLE IF EXISTS staging_airport_codes;
CREATE TABLE staging_airport_codes (
    identifier VARCHAR,
    type VARCHAR,
    name VARCHAR,
    elevation NUMERIC,
    continent VARCHAR,
    iso_country VARCHAR,
    iso_region VARCHAR,
    municipality VARCHAR,
    gps_code VARCHAR,
    iata_code VARCHAR,
    local_code VARCHAR,
    coordinates VARCHAR
)

DROP TABLE IF EXISTS staging_us_states_mapping;
CREATE TABLE staging_us_states_mapping (
    state_code VARCHAR,
    state VARCHAR
)

DROP TABLE IF EXISTS staging_countries_mapping;
CREATE TABLE staging_countries_mapping (
    country_code VARCHAR,
    country VARCHAR
)

DROP TABLE IF EXISTS staging_travel_mode_mapping;
CREATE TABLE staging_travel_mode_mapping (
    travel_code VARCHAR,
    mode VARCHAR
)

DROP TABLE IF EXISTS staging_us_ports_mapping;
CREATE TABLE staging_us_ports_mapping (
    port_code VARCHAR,
    port VARCHAR,
    us_state_code VARCHAR
)

DROP TABLE IF EXISTS staging_visa_mapping;
CREATE TABLE staging_visa_mapping (
    visa_code VARCHAR,
    visa_type VARCHAR
)

DROP TABLE IF EXISTS staging_visitor_arrivals;
CREATE TABLE staging_visitor_arrivals (
    id VARCHAR,
    cicid VARCHAR,
    i94yr INTEGER,
    i94mon INTEGER,
    i94cit INTEGER,
    i94res INTEGER,
    i94port VARCHAR,
    arrdate INTEGER,
    i94mode INTEGER,
    i94addr VARCHAR,
    depdate INTEGER,
    i94bir INTEGER,
    i94visa INTEGER,
    count INTEGER,
    dtadfile INTEGER,
    visapost VARCHAR,
    occup VARCHAR,
    entdepa VARCHAR,
    entdepd VARCHAR,
    entdepu VARCHAR,
    matflag VARCHAR,
    biryear INTEGER,
    dtaddto INTEGER,
    gender VARCHAR,
    insnum VARCHAR,
    airline VARCHAR,
    admnum INTEGER,
    fltno VARCHAR,
    visatype VARCHAR
)

--------------------------------------------------------------------------------
------------------------- DIMENSIONAL MODEL ------------------------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_port;
CREATE TABLE dim_port (
    port_id VARCHAR(16) NOT NULL,
    state_code VARCHAR(16) NOT NULL SORTKEY,
    city VARCHAR(64) NOT NULL,
	CONSTRAINT port_pkey PRIMARY KEY (port_id)
) DISTSTYLE ALL;

DROP TABLE IF EXISTS dim_us_city;
CREATE TABLE dim_us_city (
    city_id INTEGER IDENTITY(1,1),
    city VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    median_age NUMERIC(5,2),
    male_population INTEGER,
    female_population INTEGER,
    total_population INTEGER,
    number_of_veterans INTEGER,
    foreign_born INTEGER,
    average_household_size NUMERIC(5,2),
    state_code VARCHAR(16) NOT NULL SORTKEY,
    hispanic_population INTEGER,
    asian_population INTEGER,
    african_american_population INTEGER,
    native_population INTEGER,
    white_population INTEGER,
	CONSTRAINT city_pkey PRIMARY KEY (city_id)
) DISTSTYLE ALL;

DROP TABLE IF EXISTS dim_us_state;
CREATE TABLE dim_us_state (
    state_id INTEGER IDENTITY(1,1),
    state VARCHAR(64) NOT NULL,
    median_age NUMERIC(5,2),
    male_population INTEGER,
    female_population INTEGER,
    total_population INTEGER,
    number_of_veterans INTEGER,
    foreign_born INTEGER,
    average_household_size NUMERIC(5,2),
    state_code VARCHAR(16) NOT NULL,
    hispanic_population INTEGER,
    asian_population INTEGER,
    african_american_population INTEGER,
    native_population INTEGER,
    white_population INTEGER,
	CONSTRAINT state_pkey PRIMARY KEY (state_id)
) DISTSTYLE ALL;

DROP TABLE IF EXISTS dim_country;
CREATE TABLE dim_country (
    country_id INTEGER IDENTITY(1,1),
	country VARCHAR(64) NOT NULL,
	region VARCHAR(64) NOT NULL,
	happiness_score NUMERIC(10,5),
	economy_factor NUMERIC(10,5),
	family_factor NUMERIC(10,5),
	health_factor NUMERIC(10,5),
	freedom_factor NUMERIC(10,5),
	government_trust_factor NUMERIC(10,5),
	generosity_factor NUMERIC(10,5),
	region_happiness_score NUMERIC(10,5),
	CONSTRAINT country_pkey PRIMARY KEY (country_id)
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_id DATE NOT NULL SORTKEY,
    "day" SMALLINT NOT NULL,
    "week" SMALLINT NOT NULL,
    "month" SMALLINT NOT NULL,
    "year" SMALLINT NOT NULL,
    weekday  SMALLINT NOT NULL,
    CONSTRAINT date_pkey PRIMARY KEY (date_id)
) DISTSTYLE ALL;

DROP TABLE IF EXISTS fact_visitor_arrival;
CREATE TABLE fact_visitor_arrival (
    arrival_id INTEGER IDENTITY(1,1),
    arrival_date DATE NOT NULL,
    departure_date DATE,
    citizenship_country_id INTEGER,
    residency_country_id INTEGER,
    arrival_port_id VARCHAR(16),
    arrival_mode VARCHAR(16),
    destination_state_id INTEGER,
    age INTEGER,
    visa_type VARCHAR(16),
    date_added DATE,
    visa_issued_department VARCHAR(16),
    occupation_at_arrival VARCHAR(16),
    allowed_date DATE,
    gender VARCHAR(16),
    airline VARCHAR(32),
    flight_number VARCHAR(16),
    visatype VARCHAR(8)
)

