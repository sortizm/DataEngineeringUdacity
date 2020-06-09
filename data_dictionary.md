## Port dimension (dim_port)
* port_id: dimension id
* state_id: id of the state where the port is located (dim_us_state)
* city_id: id of the city where the port is located (dim_us_city)

## US State dimension (dim_us_state)
* state_id: dimension id
* state: name of the state
* male_population: amount of people in the state that are male
* female_population: amount of people in the state that are female
* total_population: total amount of people in the state
* number_of_veterans: amount of people in the state that are veterans
* foreign_born: amount of people in the state that are foreign born
* average_household_size: average size of a household in the state
* state_code: two letters code identifying the state
* hispanic_population:  amount of people in the state of hispanic ethnicity
* asian_population:  amount of people in the state of asian ethnicity
* african_american_population:  amount of people in the state of black ethnicity
* native_population:  amount of people in the state of native american ethnicity
* white_population:   amount of people in the state of white ethnicity

## US City dimension (dim_us_city)
* city_id: dimension id
* city: name of the city
* state: name of the state the city belongs to
* male_population: amount of people in the city that are male
* female_population: amount of people in the city that are female
* total_population: total amount of people in the city 
* number_of_veterans: amount of people in the city that are veterans
* foreign_born: amount of people in the city that are foreign born
* average_household_size: average size of a household in the city 
* state_code: two letters code identifying the state the city is in
* hispanic_population:  amount of people in the city of hispanic ethnicity
* asian_population:  amount of people in the city of asian ethnicity
* african_american_population:  amount of people in the city of black ethnicity
* native_population:  amount of people in the city of native american ethnicity
* white_population:   amount of people in the city of white ethnicity

## Country dimension (dim_country)  -- Add diststyle all
* country_id: dimension id
* country: name of the country
* region: region the country is in
* happiness_score: happiness score of the country (decimal number)
* economy_factor: contribution of economy factor in the happiness score
* family_factor: contribution of family life factor in the happiness score
* health_factor: contribution of health and quality of life factor in the happiness score
* freedom_factor: contribution of freedom factor in the happiness score
* government_trust_factor: contribution of trust in the government factor in the happiness score
* generosity_factor: contribution of generosity factor in the happiness score
* region_happiness_score: happiness score of the region where the country is located

## Date dimension (dim_date)
* date_id: dimension id (also actual date in date type)
* day: day of the month
* week: week of the month
* month: month
* year: year
* weekday: day of the week (numeric)

## Visitor arrival fact (fact_visitor_arrival)
* arrival_id: fact id
* cic_id: id kept from the source for data lineage (to help traceability in case of wrong data in dimensions)
* arrival_date: date the arrival happened
* departure_date: date when the visitor will leave
* citizenship_country_id: id of the country from which the visitor is citizen
* residency_country_id: id of the country from which the visitor is resident
* age: age of the visitor
* gender: gender of the visitor
* occupation_at_arrival: occupation the person will work on. For longer stays.
* destination_state_id: state id of the state where the visitor declares will first stay.
* arrival_city_id: id of city of arrival (derived using the port)
* arrival_port_id: id of the port of arrival
* arrival_mode: how the visitor arrived (air, sea, land)
* visa_class: class of visa used (tourism, business, student)
* visa_type: specific visa used (B2, WT, etc...)
* visa_issued_department: department that issued the visa
* airline: airline used to fly into the US
* flight_number: number of the flight used to fly into the US