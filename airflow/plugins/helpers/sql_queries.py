class SqlQueries:
    port_table_insert = ("""
    SELECT
        port.port_id,
        state.state_id AS state_id,
        city.city_id AS city_id
    FROM (
        SELECT
            port_code AS port_id,
            us_state_code AS state_code,
            port AS city
        FROM staging_us_ports_mapping
        
        UNION
        
        SELECT
            identifier AS port_id,
            SPLIT_PART(iso_region, '-', 2) AS state_code,
            municipality AS city
        FROM staging_airport_codes
        WHERE iso_country = 'US' AND identifier IS NOT NULL
        
        UNION
        
        SELECT
            iata_code AS port_id,
            SPLIT_PART(iso_region, '-', 2) AS state_code,
            municipality AS city
        FROM staging_airport_codes
        WHERE iso_country = 'US' AND iata_code IS NOT NULL
        
        UNION
        
        SELECT
            local_code AS port_id,
            SPLIT_PART(iso_region, '-', 2) AS state_code,
            municipality AS city
        FROM staging_airport_codes
        WHERE iso_country = 'US' AND local_code IS NOT NULL
    ) port
    LEFT JOIN dim_us_state state ON state.state_code = port.state_code
    LEFT JOIN dim_us_city city ON UPPER(city.city) = UPPER(port.city) AND city.state_code = port.state_code
    WHERE port.port_id IS NOT NULL AND port.port_id != ''
    """)

    us_city_table_insert = ("""
    (city, state, median_age, male_population, female_population,
        total_population, number_of_veterans, foreign_born,
        average_household_size, state_code, hispanic_population,
        asian_population, african_american_population, native_population,
        white_population )
    SELECT
        c.city,
        c.state,
        c.median_age,
        c.male_population,
        c.female_population,
        c.total_population,
        c.number_of_veterans,
        c.foreign_born,
        c.average_household_size,
        c.state_code,
        h.race_count as hispanic_population,
        a.race_count as asian_population,
        b.race_count as african_american_population,
        n.race_count as native_population,
        w.race_count as white_population
    FROM staging_us_cities c
    JOIN (SELECT city, state, race, race_count FROM staging_us_cities where race LIKE 'Hispanic %') h
        ON h.city = c.city AND h.state = c.state
    JOIN (SELECT city, state, race, race_count FROM staging_us_cities where race = 'Asian') a
        ON a.city = c.city AND a.state = c.state
    JOIN (SELECT city, state, race, race_count FROM staging_us_cities where race LIKE 'Black %') b
        ON b.city = c.city AND b.state = c.state
    JOIN (SELECT city, state, race, race_count FROM staging_us_cities where race LIKE 'American %') n
        ON n.city = c.city AND n.state = c.state
    JOIN (SELECT city, state, race, race_count FROM staging_us_cities where race = 'White') w
        ON w.city = c.city AND w.state = c.state
    GROUP BY c.city, c.state, c.median_age, c.state, c.city, c.male_population, c.female_population,
             c.total_population, c.number_of_veterans, c.foreign_born, c.average_household_size,
             c.state_code, hispanic_population, asian_population, african_american_population,
             native_population, white_population;
    """)

    us_state_table_insert = ("""
    (state, male_population, female_population,
        total_population, number_of_veterans, foreign_born,
        average_household_size, state_code, hispanic_population,
        asian_population, african_american_population, native_population,
        white_population )
    SELECT
        state,
        SUM(male_population) AS male_population,
        SUM(female_population) AS female_population,
        SUM(total_population) AS total_population,
        SUM(number_of_veterans) AS number_of_veterans,
        SUM(foreign_born) AS foreign_born,
        AVG(average_household_size) AS average_household_size,
        state_code,
        SUM(hispanic_population) AS hispanic_population,
        SUM(asian_population) AS asian_population,
        SUM(african_american_population) AS african_american_population,
        SUM(native_population) AS native_population,
        SUM(white_population) AS white_population
    FROM dim_us_city
    GROUP BY state, state_code
    """)

    country_table_insert = ("""
    (country, region, happiness_score, economy_factor, family_factor,
        health_factor, freedom_factor, government_trust_factor,
        generosity_factor, region_happiness_score)
    SELECT 
        c.country,
        c.region,
        c.happiness_score,
        c.economy_factor,
        c.family_factor,
        c.health_factor,
        c.freedom_factor,
        c.government_trust_factor,
        c.generosity_factor,
        r.happiness_score
    FROM staging_world_happiness c
    JOIN (SELECT region, AVG(happiness_score) AS happiness_score FROM staging_world_happiness GROUP BY region) r
        ON r.region = c.region
    """)

    date_table_insert = ("""
    SELECT d.date_id,
        extract(day from d.date_id) AS day,
        extract(week from d.date_id) AS week,
        extract(month from d.date_id) AS month,
        extract(year from d.date_id) AS year,
        extract(dow from d.date_id) AS weekday
    FROM (
        SELECT '1960-1-1'::date + (arrdate * interval '1 day') AS date_id
        FROM staging_visitor_arrivals_mapped
        WHERE arrdate IS NOT NULL) d

    UNION

    SELECT d.date_id,
        extract(day from d.date_id) AS day,
        extract(week from d.date_id) AS week,
        extract(month from d.date_id) AS month,
        extract(year from d.date_id) AS year,
        extract(dow from d.date_id) AS weekday
    FROM (
        SELECT '1960-1-1'::date + (depdate * interval '1 day') AS date_id
        FROM staging_visitor_arrivals_mapped
        WHERE depdate IS NOT NULL) d
    """)

    visitor_arrival_mapped_staging_table_insert = ("""
    SELECT
        CAST (v.cicid AS INTEGER),
        CAST(v.i94yr AS INTEGER),
        CAST(v.i94mon AS INTEGER),
        c1.country AS i94cit,
        c2.country AS i94res,
        v.i94port,
        CAST(v.arrdate AS INTEGER),
        CAST(v.i94mode AS INTEGER),
        v.i94addr,
        CAST(v.depdate AS INTEGER),
        CAST(v.i94bir AS INTEGER),
        CAST(v.i94visa AS INTEGER),
        CAST(v."count" AS INTEGER),
        CASE
            WHEN v.dtadfile ~ '^[0-9]+$' THEN CAST(v.dtadfile AS INTEGER)
        END AS dtadfile,
        v.visapost,
        v.occup,
        v.entdepa,
        v.entdepd,
        v.entdepu,
        v.matflag,
        CAST(v.biryear AS INTEGER),
        CASE
            WHEN v.dtaddto ~ '^[0-9]+$' THEN CAST(v.dtaddto AS INTEGER)
        END AS dtaddto,
        v.gender,
        v.insnum,
        v.airline,
        CAST(v.admnum AS BIGINT),
        v.fltno,
        v.visatype
    FROM staging_visitor_arrivals v
    LEFT JOIN staging_countries_mapping c1 ON c1.country_code = CAST(v.i94cit AS INTEGER)
    LEFT JOIN staging_countries_mapping c2 ON c2.country_code = CAST(v.i94cit AS INTEGER)
    """)

    visitor_arrival_table_insert = ("""
    (cic_id, arrival_date, departure_date,
     citizenship_country_id, residency_country_id, age, gender,
     occupation_at_arrival, destination_state_id, arrival_city_id,
     arrival_port_id, arrival_mode, visa_class, visa_type,
     visa_issued_department, airline, flight_number)
    SELECT
        v.cicid AS cic_id,
        arr_date.date_id AS arrival_date,
        dep_date.date_id AS departure_date,
        cit_country.country_id AS citizenship_country_id,
        res_country.country_id AS residency_country_id,
        v.i94bir AS age,
        v.gender,
        v.occup AS occupation_at_arrival,
        state.state_id AS destination_state_id,
        port.city_id AS arrival_city_id,
        port.port_id AS arrival_port_id,
        travel_mode.mode AS arrival_mode,
        visa.visa_type AS visa_class,
        visatype AS visa_type,
        v.visapost AS visa_issued_department,
        v.airline,
        v.fltno AS flight_number
    FROM staging_visitor_arrivals_mapped v
    JOIN dim_date arr_date ON arr_date.date_id = '1960-1-1'::date + (v.arrdate * interval '1 day')
    LEFT JOIN dim_date dep_date ON dep_date.date_id = '1960-1-1'::date + (v.depdate * interval '1 day')
    LEFT JOIN dim_country cit_country ON UPPER(cit_country.country) = UPPER(v.i94cit)
    LEFT JOIN dim_country res_country ON UPPER(res_country.country) = UPPER(v.i94res)
    LEFT JOIN dim_us_state state ON state.state_code = v.i94addr
    LEFT JOIN dim_port port ON port.port_id = v.i94port
    LEFT JOIN staging_travel_mode_mapping travel_mode ON travel_mode.travel_code = v.i94mode
    LEFT JOIN staging_visa_mapping visa ON visa.visa_code = v.i94visa
    """)
