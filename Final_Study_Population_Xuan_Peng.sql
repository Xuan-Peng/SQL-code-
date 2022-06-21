USE new_schema;

-- 1.Find the index event （within 2017 to 2020, only include specific ICD, exclude laterality code = 4）

-- Create a temporary table to find all patient eyes with right ICD-10 code within 2017 to 2020 and excluding laterality code = 4
CREATE TEMPORARY TABLE patient_w_right_code
SELECT DISTINCT *
FROM (SELECT * FROM take_home_patient_condition WHERE laterality_code != 4 AND diagnosis_date BETWEEN "2017-01-01" AND "2020-12-31") a
WHERE 
	condition_code LIKE "H44.00%" 
	OR condition_code LIKE "H44.01%"
	OR condition_code LIKE "H44.02%" 
	OR condition_code LIKE "H44.11%";

CREATE TEMPORARY TABLE index_event_table
SELECT patient_guid, laterality_code, MIN(diagnosis_date) AS index_event
FROM patient_w_right_code
GROUP BY patient_guid, laterality_code;

-- Now, we can cacluate the number of patients and the number of eyes we have for the index event table
SELECT 
	COUNT(DISTINCT patient_guid) AS num_patients,    										 -- number of patients = 76,905
    SUM(CASE WHEN laterality_code = 1 OR laterality_code = 2 THEN 1 ELSE 2 END) AS num_eyes  -- number of eyes = 103,549 (Split records with laterality code = 3 to 1 and 2 during calculation, but there might be duplicates which would be solved later)
FROM index_event_table;																	    


-- 2. Select patients who are 18 and older on the index date and Split bilaterality records to right and left eye

-- Create a temporary table to find all records of patient over 18 years old 
CREATE TEMPORARY TABLE index_event_over_18
SELECT i.patient_guid, laterality_code, index_event
FROM index_event_table i JOIN take_home_patient y ON i.patient_guid = y.patient_guid
WHERE LEFT(index_event,4) - birth_year >= 18 AND birth_year IS NOT NULL AND birth_year != 0;

CREATE TEMPORARY TABLE index_event_over_18_b  -- This is the same table as above for avoiding reopen temporary table problem; 
SELECT i.patient_guid, laterality_code, index_event
FROM index_event_table i JOIN take_home_patient y ON i.patient_guid = y.patient_guid
WHERE LEFT(index_event,4) - birth_year >= 18 AND birth_year IS NOT NULL AND birth_year != 0;

CREATE TEMPORARY TABLE record_3_split
SELECT 	
	patient_guid, 
    CASE WHEN laterality_code = 3 THEN 1 END laterality_code,
    index_event
FROM index_event_over_18
WHERE laterality_code = 3
UNION
SELECT 
	patient_guid, 
    CASE WHEN laterality_code = 3 THEN 2 END laterality_code,
    index_event
FROM index_event_over_18_b
WHERE laterality_code = 3;

-- Create a table combining records with laterality code 1, 2 and the new records from laterality code 3 splitting
CREATE TEMPORARY TABLE index_event_over_18_12
SELECT patient_guid, laterality_code, index_event
FROM index_event_over_18
WHERE laterality_code != 3   							-- only include records with laterality code 1 and 2
UNION
SELECT patient_guid, laterality_code, index_event 		-- records from splitting bilaterality code
FROM record_3_split;

CREATE TEMPORARY TABLE index_event_over18
SELECT patient_guid, laterality_code, MIN(index_event) AS index_event   -- After splitting, there might be two index date for the same eye. Therefore, we need to find the minimum date again.
FROM index_event_over_18_12
GROUP BY patient_guid, laterality_code;

-- Now, we can cacluate the number of patients and the number of eyes we have for the second round of filtering;
SELECT 
	COUNT(DISTINCT patient_guid) AS num_patients,   -- number of patients is 75,119
    COUNT(*) AS num_eyes                   			-- number of patient eyes is 95,210
FROM index_event_over18;


-- 3.Only include patient eyes that received an intravitreal injection within 21 days before the endophthalmitis index event

-- First, split injection records with laterality code = 3, so that the records can match with the index event.
CREATE TEMPORARY TABLE injection_3_split
SELECT DISTINCT 
	patient_guid,
    CASE WHEN laterality_code = 3 THEN 1 END AS laterality_code,
    concept_id,
    injection_date
FROM take_home_patient_concept_date
WHERE laterality_code = 3
UNION
SELECT DISTINCT 
	patient_guid,
    CASE WHEN laterality_code = 3 THEN 2 END AS laterality_code,
    concept_id,
    injection_date
FROM take_home_patient_concept_date
WHERE laterality_code = 3;

CREATE TEMPORARY TABLE injection_w_split_3
SELECT DISTINCT *
FROM take_home_patient_concept_date
WHERE laterality_code = 1 OR laterality_code = 2
UNION 
SELECT *
FROM injection_3_split;

-- Create a table only include the first record of injection, if there's more than one injection within 21 days, select the earliest record of injection.
CREATE TEMPORARY TABLE index_event_over_18_within_21_w_rank
SELECT d.patient_guid, d.laterality_code, injection_date, index_event, DENSE_RANK() OVER (PARTITION BY d.patient_guid, d.laterality_code ORDER BY injection_date) r
FROM injection_w_split_3 d JOIN index_event_over18 i ON d.patient_guid = i.patient_guid AND d.laterality_code = i.laterality_code 
	AND DATEDIFF(index_event, injection_date) BETWEEN 0 AND 21;

CREATE TEMPORARY TABLE index_event_over_18_within_21
SELECT DISTINCT patient_guid, laterality_code, injection_date, index_event
FROM index_event_over_18_within_21_w_rank
WHERE r = 1;    -- Select the earliest injection within 21 days prior to the endophthalmitis index event

-- Now, we can cacluate the number of patients and the number of eyes we have for the third round of filtering
SELECT 
	COUNT(DISTINCT patient_guid) AS num_patients,   -- number of patients is 8,585
    COUNT(*) AS num_eyes                   			-- number of patient eyes is 8,744
FROM index_event_over_18_within_21;

    
-- 4.Excluded patients who have CPT other than 67028 within 90 days prior to the index event;
CREATE TEMPORARY TABLE patients_other_surgeries_prior_90days
SELECT DISTINCT i.patient_guid
FROM index_event_over_18_within_21 i JOIN take_home_patient_procedure p 
	ON i.patient_guid = p.patient_guid 
	AND DATEDIFF(index_event, procedure_date) BETWEEN 0 AND 90 
WHERE p.procedure_cpt_code BETWEEN 65091 AND 67027 OR p.procedure_cpt_code BETWEEN 67029 AND 68899;

CREATE TEMPORARY TABLE index_event_over_18_within_21_nosurg
SELECT patient_guid, laterality_code, injection_date, index_event
FROM index_event_over_18_within_21
WHERE patient_guid NOT IN (SELECT patient_guid FROM patients_other_surgeries_prior_90days);

-- Now, we can cacluate the number of patients and the number of eyes we have for the fourth round of filtering
SELECT 
	COUNT(DISTINCT patient_guid) AS num_patients,   -- number of patients is 4,719
    COUNT(*) AS num_eyes                   			-- number of patient eyes is 4,838
FROM index_event_over_18_within_21_nosurg;


-- 5.Exclude patients who had endophthalmitis diagnosis before 2017
CREATE TEMPORARY TABLE had_infected_before_17 
SELECT DISTINCT patient_guid
FROM (SELECT * FROM take_home_patient_condition WHERE diagnosis_date < "2017-01-01") before17
WHERE condition_code LIKE "360.0%" OR condition_code = "360.12" 
	OR condition_code LIKE "H44.00%" OR condition_code LIKE "H44.01%" OR condition_code LIKE "H44.02%" OR condition_code LIKE "H44.11%";

CREATE TEMPORARY TABLE final_study_population
SELECT a.patient_guid, laterality_code,injection_date, index_event
FROM index_event_over_18_within_21_nosurg a LEFT JOIN (SELECT * FROM had_infected_before_17) b ON a.patient_guid = b.patient_guid
WHERE b.patient_guid IS NULL;

-- Now, we can cacluate the number of patients and the number of eyes we have for the last round of filtering
SELECT 
	COUNT(DISTINCT patient_guid) AS num_patients,   -- number of patients is 3,259
    COUNT(*) AS num_eyes                   			-- number of patient eyes is 3,309
FROM final_study_population;

SELECT *
FROM final_study_population;

-- CREATE TEMPORARY TABLE population
-- SELECT DISTINCT a.patient_guid, a.laterality_code, concept_id, a.injection_date, index_event, DATEDIFF(index_event,a.injection_date) AS differences_in_days
-- FROM final_study_population a JOIN injection_w_split_3 b 
-- 	ON a.patient_guid = b.patient_guid AND 
--     a.laterality_code = b.laterality_code AND 
--     a.injection_date = b.injection_date;

-- (SELECT * FROM population GROUP BY patient_guid, laterality_code, injection_date HAVING COUNT(*) = 1);

CREATE TEMPORARY TABLE total_num_eyes_yr_agent
SELECT YEAR(index_event) AS "year", concept_id, COUNT(*) AS total_num_eyes
FROM (SELECT * FROM population GROUP BY patient_guid, laterality_code, injection_date HAVING COUNT(*) = 1) unique_records
GROUP BY 1,2
ORDER BY 1,2;


CREATE TEMPORARY TABLE population
SELECT DISTINCT a.patient_guid, a.laterality_code, concept_id, YEAR(index_event) AS year_index, DATEDIFF(index_event,a.injection_date) AS differences_in_days
FROM final_study_population a JOIN injection_w_split_3 b 
	ON a.patient_guid = b.patient_guid AND 
    a.laterality_code = b.laterality_code AND 
    a.injection_date = b.injection_date
GROUP BY a.patient_guid, a.laterality_code, a.injection_date
HAVING COUNT(*) = 1 ;

SELECT *
FROM population;