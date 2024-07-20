--sql server CDM DDL Specification for OMOP Common Data Model 5.4
--drop table concept;
--drop table concept_classs;
--drop table vocabulary;
--drop table domain;
--drop table location;
--drop table care_site;
--drop table provider;
--drop table person;
--drop table visit_occurrence;
--drop table visit_detail;
--drop table relationship;
--drop table episode;
--drop table note;
--drop table cdm_source;
--drop table cohort_definition;
--drop table concept_ancestor;
--drop table concept_synonym;
--drop table condition_era;
--drop table drug_era;
--drop table episode_event;
--drop table observation_period;
--drop table concept_relationship;
--drop table dose_era;
--drop table fact_relationship;
--drop table metadata;
--drop table death;
--drop table note_nlp;
--drop table source_to_concept_map;
--drop table cost;
--drop table drug_strength;
--drop table specimen;
--drop table condition_occurrence;
--drop table drug_exposure;
--drop table procedure_occurrence;
--drop table device_exposure;
--drop table payer_plan_period;
--drop table observation;
--drop table measurement;
--drop table cohort;




--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE person (person_id bigint NOT NULL,gender_concept_id bigint NOT NULL,year_of_birth integer NOT NULL,month_of_birth integer,day_of_birth  integer,birth_timestamp timestamp,race_concept_id bigint NOT NULL,ethnicity_concept_id bigint NOT NULL,location_id bigint,provider_id bigint,care_site_id bigint,person_source_value string,gender_source_value string,gender_source_concept_id bigint,race_source_value string,race_source_concept_id bigint,ethnicity_source_value string,ethnicity_source_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE observation_period (observation_period_id bigint NOT NULL,person_id bigint NOT NULL,observation_period_start_date timestamp NOT NULL,observation_period_end_date timestamp NOT NULL,period_type_concept_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE visit_occurrence (visit_occurrence_id bigint NOT NULL, person_id bigint NOT NULL, visit_concept_id bigint NOT NULL, visit_start_date timestamp NOT NULL, visit_start_timestamp timestamp, visit_end_date timestamp NOT NULL, visit_end_timestamp timestamp, visit_type_concept_id bigint NOT NULL, provider_id bigint, care_site_id bigint, visit_source_value string, visit_source_concept_id bigint, admitted_from_concept_id bigint, admitted_from_source_value string, discharged_to_concept_id bigint, discharged_to_source_value string, preceding_visit_occurrence_id bigint ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE visit_detail (visit_detail_id bigint NOT NULL, person_id bigint NOT NULL, visit_detail_concept_id bigint NOT NULL, visit_detail_start_date timestamp NOT NULL, visit_detail_start_timestamp timestamp, visit_detail_end_date timestamp NOT NULL, visit_detail_end_timestamp timestamp, visit_detail_type_concept_id bigint NOT NULL, provider_id bigint, care_site_id bigint, visit_detail_source_value string, visit_detail_source_concept_id bigint, admitted_from_concept_id bigint, admitted_from_source_value string, discharged_to_source_value string, discharged_to_concept_id bigint, preceding_visit_detail_id bigint, parent_visit_detail_id bigint, visit_occurrence_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE condition_occurrence (condition_occurrence_id bigint NOT NULL, person_id bigint NOT NULL, condition_concept_id bigint NOT NULL, condition_start_date timestamp NOT NULL, condition_start_timestamp timestamp, condition_end_date timestamp, condition_end_timestamp timestamp, condition_type_concept_id bigint NOT NULL, condition_status_concept_id bigint, stop_reason string, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint, condition_source_value string, condition_source_concept_id bigint, condition_status_source_value string ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE drug_exposure (drug_exposure_id bigint NOT NULL, person_id bigint NOT NULL, drug_concept_id bigint NOT NULL, drug_exposure_start_date timestamp NOT NULL, drug_exposure_start_timestamp timestamp, drug_exposure_end_date timestamp NOT NULL, drug_exposure_end_timestamp timestamp, verbatim_end_date timestamp, drug_type_concept_id bigint NOT NULL, stop_reason string, refills  integer, quantity float, days_supply  integer, sig string, route_concept_id bigint, lot_number string, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint, drug_source_value string, drug_source_concept_id bigint, route_source_value string, dose_unit_source_value string ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE procedure_occurrence (procedure_occurrence_id bigint NOT NULL, person_id bigint NOT NULL, procedure_concept_id bigint NOT NULL, procedure_date timestamp NOT NULL, procedure_timestamp timestamp, procedure_end_date timestamp, procedure_end_timestamp timestamp, procedure_type_concept_id bigint NOT NULL, modifier_concept_id bigint, quantity  integer, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint, procedure_source_value string, procedure_source_concept_id bigint, modifier_source_value string ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE device_exposure (device_exposure_id bigint NOT NULL, person_id bigint NOT NULL, device_concept_id bigint NOT NULL, device_exposure_start_date timestamp NOT NULL, device_exposure_start_timestamp timestamp, device_exposure_end_date timestamp, device_exposure_end_timestamp timestamp, device_type_concept_id bigint NOT NULL, unique_device_id string, production_id string, quantity  integer, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint, device_source_value string, device_source_concept_id bigint, unit_concept_id bigint, unit_source_value string, unit_source_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE measurement (measurement_id bigint NOT NULL, person_id bigint NOT NULL, measurement_concept_id bigint NOT NULL, measurement_date timestamp NOT NULL, measurement_timestamp timestamp, measurement_time string, measurement_type_concept_id bigint NOT NULL, operator_concept_id bigint, value_as_number float, value_as_concept_id bigint, unit_concept_id bigint, range_low float, range_high float, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint, measurement_source_value string, measurement_source_concept_id bigint, unit_source_value string, unit_source_concept_id bigint, value_source_value string, measurement_event_id bigint, meas_event_field_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE observation (observation_id bigint NOT NULL, person_id bigint NOT NULL, observation_concept_id bigint NOT NULL, observation_date timestamp NOT NULL, observation_timestamp timestamp, observation_type_concept_id bigint NOT NULL, value_as_number float, value_as_string string, value_as_concept_id bigint, qualifier_concept_id bigint, unit_concept_id bigint, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint, observation_source_value string, observation_source_concept_id bigint, unit_source_value string, qualifier_source_value string, value_source_value string, observation_event_id bigint, obs_event_field_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE death (person_id bigint NOT NULL, death_date timestamp NOT NULL, death_timestamp timestamp, death_type_concept_id bigint, cause_concept_id bigint, cause_source_value string, cause_source_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE note (note_id bigint NOT NULL, person_id bigint NOT NULL, note_date timestamp NOT NULL, note_timestamp timestamp, note_type_concept_id bigint NOT NULL, note_class_concept_id bigint NOT NULL, note_title string, note_text string NOT NULL, encoding_concept_id bigint NOT NULL, language_concept_id bigint NOT NULL, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint, note_source_value string, note_event_id bigint, note_event_field_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE note_nlp (note_nlp_id bigint NOT NULL, note_id bigint NOT NULL, section_concept_id bigint, snippet string, offset string, lexical_variant string NOT NULL, note_nlp_concept_id bigint, note_nlp_source_concept_id bigint, nlp_system string, nlp_date timestamp NOT NULL, nlp_timestamp timestamp, term_exists string, term_temporal string, term_modifiers string ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE specimen (specimen_id bigint NOT NULL, person_id bigint NOT NULL, specimen_concept_id bigint NOT NULL, specimen_type_concept_id bigint NOT NULL, specimen_date timestamp NOT NULL, specimen_timestamp timestamp, quantity float, unit_concept_id bigint, anatomic_site_concept_id bigint, disease_status_concept_id bigint, specimen_source_id string, specimen_source_value string, unit_source_value string, anatomic_site_source_value string, disease_status_source_value string ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE fact_relationship (domain_concept_id_1 integer NOT NULL, fact_id_1 integer NOT NULL, domain_concept_id_2 integer NOT NULL, fact_id_2 integer NOT NULL, relationship_concept_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE location (location_id bigint NOT NULL, address_1 string, address_2 string, city string, state string, zip string, county string, location_source_value string, country_concept_id bigint, country_source_value string, latitude float, longitude float ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE care_site (care_site_id bigint NOT NULL, care_site_name string, place_of_service_concept_id bigint, location_id bigint, care_site_source_value string, place_of_service_source_value string ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE provider (provider_id bigint NOT NULL, provider_name string, npi string, dea string, specialty_concept_id bigint, care_site_id bigint, year_of_birth  integer, gender_concept_id bigint, provider_source_value string, specialty_source_value string, specialty_source_concept_id bigint, gender_source_value string, gender_source_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE payer_plan_period (payer_plan_period_id bigint NOT NULL, person_id bigint NOT NULL, payer_plan_period_start_date timestamp NOT NULL, payer_plan_period_end_date timestamp NOT NULL, payer_concept_id bigint, payer_source_value string, payer_source_concept_id bigint, plan_concept_id bigint, plan_source_value string, plan_source_concept_id bigint, sponsor_concept_id bigint, sponsor_source_value string, sponsor_source_concept_id bigint, family_source_value string, stop_reason_concept_id bigint, stop_reason_source_value string, stop_reason_source_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cost (cost_id bigint NOT NULL, cost_event_id bigint NOT NULL, cost_domain_id string NOT NULL, cost_type_concept_id bigint NOT NULL, currency_concept_id bigint, total_charge float, total_cost float, total_paid float, paid_by_payer float, paid_by_patient float, paid_patient_copay float, paid_patient_coinsurance float, paid_patient_deductible float, paid_by_primary float, paid_ingredient_cost float, paid_dispensing_fee float, payer_plan_period_id bigint, amount_allowed float, revenue_code_concept_id bigint, revenue_code_source_value string, drg_concept_id bigint, drg_source_value string ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE drug_era (drug_era_id bigint NOT NULL, person_id bigint NOT NULL, drug_concept_id bigint NOT NULL, drug_era_start_date timestamp NOT NULL, drug_era_end_date timestamp NOT NULL, drug_exposure_count  integer, gap_days  integer ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE dose_era (dose_era_id bigint NOT NULL, person_id bigint NOT NULL, drug_concept_id bigint NOT NULL, unit_concept_id bigint NOT NULL, dose_value float NOT NULL, dose_era_start_date timestamp NOT NULL, dose_era_end_date timestamp NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE condition_era (condition_era_id bigint NOT NULL, person_id bigint NOT NULL, condition_concept_id bigint NOT NULL, condition_era_start_date timestamp NOT NULL, condition_era_end_date timestamp NOT NULL, condition_occurrence_count  integer ) using iceberg;
--HINT DISTRIBUTE ON KEY (person_id)
CREATE TABLE episode (episode_id bigint NOT NULL, person_id bigint NOT NULL, episode_concept_id bigint NOT NULL, episode_start_date timestamp NOT NULL, episode_start_timestamp timestamp, episode_end_date timestamp, episode_end_timestamp timestamp, episode_parent_id bigint, episode_number  integer, episode_object_concept_id bigint NOT NULL, episode_type_concept_id bigint NOT NULL, episode_source_value string, episode_source_concept_id bigint ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE episode_event (episode_id bigint NOT NULL, event_id bigint NOT NULL, episode_event_field_concept_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE metadata (metadata_id bigint NOT NULL, metadata_concept_id bigint NOT NULL, metadata_type_concept_id bigint NOT NULL, name string NOT NULL, value_as_string string, value_as_concept_id bigint, value_as_number float, metadata_date timestamp, metadata_timestamp timestamp ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cdm_source (cdm_source_name string NOT NULL, cdm_source_abbreviation string NOT NULL, cdm_holder string NOT NULL, source_description string, source_documentation_reference string, cdm_etl_reference string, source_release_date timestamp NOT NULL, cdm_release_date timestamp NOT NULL, cdm_version string, cdm_version_concept_id bigint NOT NULL, vocabulary_version string NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept (concept_id bigint NOT NULL, concept_name string NOT NULL, domain_id string NOT NULL, vocabulary_id string NOT NULL, concept_class_id string NOT NULL, standard_concept string, concept_code string NOT NULL, valid_start_date timestamp NOT NULL, valid_end_date timestamp NOT NULL, invalid_reason string ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE vocabulary (vocabulary_id string NOT NULL, vocabulary_name string NOT NULL, vocabulary_reference string, vocabulary_version string, vocabulary_concept_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE domain (domain_id string NOT NULL, domain_name string NOT NULL, domain_concept_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_class (concept_class_id string NOT NULL, concept_class_name string NOT NULL, concept_class_concept_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_relationship (concept_id_1 integer NOT NULL, concept_id_2 integer NOT NULL, relationship_id string NOT NULL, valid_start_date timestamp NOT NULL, valid_end_date timestamp NOT NULL, invalid_reason string ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE relationship (relationship_id string NOT NULL, relationship_name string NOT NULL, is_hierarchical string NOT NULL, defines_ancestry string NOT NULL, reverse_relationship_id string NOT NULL, relationship_concept_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_synonym (concept_id bigint NOT NULL, concept_synonym_name string NOT NULL, language_concept_id bigint NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_ancestor (ancestor_concept_id bigint NOT NULL, descendant_concept_id bigint NOT NULL, min_levels_of_separation integer NOT NULL, max_levels_of_separation integer NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE source_to_concept_map (source_code string NOT NULL, source_concept_id bigint NOT NULL, source_vocabulary_id string NOT NULL, source_code_description string, target_concept_id bigint NOT NULL, target_vocabulary_id string NOT NULL, valid_start_date timestamp NOT NULL, valid_end_date timestamp NOT NULL, invalid_reason string ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE drug_strength (drug_concept_id bigint NOT NULL, ingredient_concept_id bigint NOT NULL, amount_value float, amount_unit_concept_id bigint, numerator_value float, numerator_unit_concept_id bigint, denominator_value float, denominator_unit_concept_id bigint, box_size  integer, valid_start_date timestamp NOT NULL, valid_end_date timestamp NOT NULL, invalid_reason string ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort (cohort_definition_id bigint NOT NULL, subject_id bigint NOT NULL, cohort_start_date timestamp NOT NULL, cohort_end_date timestamp NOT NULL ) using iceberg;
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort_definition (cohort_definition_id bigint NOT NULL, cohort_definition_name string NOT NULL, cohort_definition_description string, definition_type_concept_id bigint NOT NULL, cohort_definition_syntax string, subject_concept_id bigint NOT NULL, cohort_initiation_date timestamp ) using iceberg;

 -- relacionamento com a tabela person do omop
        -- indica o sistema de origem dos dados (1-sinasc, 2-sim)
 -- código do município de naturalidade da mãe
 -- código de ocupação da mãe conforme tabela do cbo (código brasileiro de ocupações).
 -- código da uf de naturalidade da mãe
 -- data de nascimento da mãe: dd mm aaaa
  -- data de nascimento da mãe em formato date
 -- escolaridade, em anos de estudo concluídos: 1 – nenhuma; 2 – 1 a 3 anos; 3 – 4 a 7 anos; 4 – 8 a 11 anos; 5 – 12 e mais; 9 – ignorado.
 -- escolaridade 2010. valores: 0 – sem escolaridade; 1 – fundamental i (1ª a 4ª série) using iceberg; 2 – fundamental ii (5ª a 8ª série) using iceberg; 3 – médio (antigo 2º grau) using iceberg; 4 – superior incompleto; 5 – superior completo; 9 – ignorado.
 -- escolaridade 2010 agregada. valores: 00 – sem escolaridade; 01 – fundamental i incompleto; 02 – fundamental i completo; 03 – fundamental ii incompleto; 04 – fundamental ii completo; 05 – ensino médio incompleto; 06 – ensino médio completo; 07 – superior incompleto; 08 – superior completo; 09 – ignorado; 10 – fundamental i incompleto ou inespecífico; 11 – fundamental ii incompleto ou inespecífico; 12 – ensino médio incompleto ou inespecífico.   
 -- situação conjugal da mãe: 1– solteira; 2– casada; 3– viúva; 4– separada judicialmente/divorciada; 5– união estável; 9– ignorada.
 -- idade da mãe
 -- se a mãe for estrangeira, constará o código do país de nascimento.
 -- 1 tipo de raça e cor da mãe: 1– branca; 2– preta; 3– amarela; 4– parda; 5– indígena.
 -- série escolar da mãe. valores de 1 a 8.
 -- idade do pai
 -- tipo do documento do responsável. valores: 1‐cnes; 2‐crm; 3‐ coren; 4‐rg; 5‐cpf.
 -- tipo de função do responsável pelo preenchimento. valores: 1– médico; 2– enfermeiro; 3– parteira; 4– funcionário do cartório; 5– outros.
 -- anomalia identificada: 1– sim; 2– não; 9– ignorado
 -- local de nascimento: 1 – hospital; 2 – outros estabelecimentos de saúde; 3 – domicílio; 4 – outros.

Create table datasus_person (person_id  bigint not null,system_source_id  integer not null, city_origin integer,mother_city integer,state_origin  integer,mother_birth_date date, mother_birth_date_source_value integer, mother_years_of_study integer,mother_education_level integer,mother_education_level_aggregated integer,mother_marital_status  integer,mother_age   integer,mother_city_of_birth integer,mother_race integer,mother_elementary_school integer,father_age   integer,responsible_document_type  integer,responsible_role_type integer,place_of_birth_type_source_value integer, care_site_of_birth_source_value integer, mother_professional_occupation integer, mother_country_of_origin integer, number_of_dead_children integer, number_of_living_children integer, number_of_previous_pregnancies integer, number_of_previous_cesareans integer, number_of_previous_normal_born integer) using iceberg;






