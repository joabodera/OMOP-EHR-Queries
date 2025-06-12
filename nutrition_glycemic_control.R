query <- "
WITH visits AS (
  SELECT 
    person_id,
    visit_occurrence_id,
    visit_start_datetime,
    visit_end_datetime,
    CAST(src_name AS INTEGER) AS src_name,
    visit_concept_id
  FROM omopcdm.visit_occurrence
  WHERE visit_concept_id = 9201
    AND visit_start_datetime IS NOT NULL
    AND visit_end_datetime IS NOT NULL
    AND CAST(src_name AS INTEGER) BETWEEN 1 AND 13
),
ordered_visits AS (
  SELECT *,
         LAG(visit_end_datetime) OVER (PARTITION BY person_id, src_name ORDER BY visit_start_datetime) AS prev_end
  FROM visits
),
flag_chains AS (
  SELECT *,
         CASE 
           WHEN prev_end IS NULL THEN 1
           WHEN visit_start_datetime - prev_end <= INTERVAL '1 hour' THEN 0
           ELSE 1
         END AS chain_break
  FROM ordered_visits
),
grouped_chains AS (
  SELECT *,
         SUM(chain_break) OVER (
           PARTITION BY person_id, src_name 
           ORDER BY visit_start_datetime 
           ROWS UNBOUNDED PRECEDING
         ) AS chain_id
  FROM flag_chains
),
collapsed_visits AS (
  SELECT 
    person_id,
    src_name,
    MIN(visit_start_datetime) AS visit_start_datetime,
    MAX(visit_end_datetime) AS visit_end_datetime,
    EXTRACT(EPOCH FROM (MAX(visit_end_datetime) - MIN(visit_start_datetime))) / 3600 AS hospital_los_hours
  FROM grouped_chains
  GROUP BY person_id, src_name, chain_id
)

-- Final output: only stays ≥ 24 hours
SELECT 
  person_id,
  src_name,
  visit_start_datetime,
  visit_end_datetime,
  ROUND(hospital_los_hours, 1) AS hospital_los_hours
FROM collapsed_visits
WHERE hospital_los_hours >= 24
ORDER BY person_id, visit_start_datetime;
"

long_los_patients <- querySql(conn, query)

head(long_los_patients)

dbWriteTable(conn, "long_los_temp", long_los_patients, temporary = TRUE, overwrite = TRUE)


query2 <- "
SELECT 
  m.measurement_id,
  m.person_id,
  m.measurement_concept_id,
  m.measurement_datetime,
  m.value_as_number,
  m.unit_concept_id,
  los.src_name,
  los.visit_start_datetime,
  los.visit_end_datetime
FROM omopcdm.measurement m
JOIN long_los_temp los ON m.person_id = los.person_id
WHERE m.measurement_concept_id IN (
  3004501, 3000483, 3031266, 3034962, 44816672, 3044242, 82947, 
  4144235, 3014053, 3011424, 3033408, 3004077, 2212359, 3037110, 4149519
)
  AND m.measurement_datetime IS NOT NULL
  AND m.measurement_datetime::time <> '00:00:00'
  AND m.measurement_datetime BETWEEN los.visit_start_datetime AND los.visit_end_datetime;
"


measurement_subset <- querySql(conn, query2)

head(measurement_subset)

colnames(measurement_subset)

str(measurement_subset)


#library(dplyr)

measurement_subset <- measurement_subset %>%
  mutate(
    VISIT_START_DATETIME = as.POSIXct(VISIT_START_DATETIME),
    MEASUREMENT_DATETIME = as.POSIXct(MEASUREMENT_DATETIME),
    elapsed_hours = as.numeric(difftime(MEASUREMENT_DATETIME, VISIT_START_DATETIME, units = "hours"))
  ) %>%
  filter(elapsed_hours >= 0, elapsed_hours < 24)


head(measurement_subset)

#library(dplyr)

glucose_counts <- measurement_subset %>%
  group_by(PERSON_ID, SRC_NAME) %>%
  summarise(n_glucose_checks = n(), .groups = "drop")

head(glucose_counts)

#library(ggplot2)

ggplot(glucose_counts, aes(x = n_glucose_checks)) +
  geom_histogram(binwidth = 1, fill = "steelblue", color = "black") +
  facet_wrap(~ SRC_NAME, scales = "free_y") +
  scale_x_continuous(limits = c(0, 25)) +  # adjust if you want tighter/looser view
  labs(
    title = "Number of Glucose Checks per Person (First 24h of Admission)",
    x = "Number of Glucose Measurements",
    y = "Number of Patients"
  ) +
  theme_minimal()

#library(dplyr)

patients_per_site <- measurement_subset %>%
  distinct(PERSON_ID, SRC_NAME) %>%
  count(SRC_NAME, name = "n_patients") %>%
  arrange(SRC_NAME)

print(patients_per_site)


# Total number of patients
total_patients <- measurement_subset %>%
  distinct(PERSON_ID) %>%
  count(name = "total_patients")

print(total_patients)

# total number of patients with glucose checks are 7955 patients

#------------Query nutrition---------------

query_nutrition <- "
SELECT 
  m.measurement_id,
  m.person_id,
  m.measurement_concept_id,
  m.measurement_datetime,
  m.value_as_number,
  m.unit_concept_id,
  los.src_name,
  los.visit_start_datetime,
  los.visit_end_datetime
FROM omopcdm.measurement m
JOIN long_los_temp los ON m.person_id = los.person_id
WHERE m.measurement_concept_id IN (4222605, 4298279, 619486, 45889493, 45889491)
  AND m.measurement_datetime IS NOT NULL
  AND m.measurement_datetime BETWEEN los.visit_start_datetime AND los.visit_end_datetime;
"

nutrition_subset <- querySql(conn, query_nutrition)

head(nutrition_subset)

# Count unique patients
total_nutrition_patients <- nutrition_subset %>%
  distinct(PERSON_ID) %>%
  count(name = "total_patients")

print(total_nutrition_patients)


# Count Patients per Site

nutrition_patients_per_site <- nutrition_subset %>%
  distinct(PERSON_ID, SRC_NAME) %>%
  count(SRC_NAME, name = "n_patients") %>%
  arrange(SRC_NAME)

print(nutrition_patients_per_site)

# Count Patients Per Measurement Concept ID:

nutrition_patients_per_measurement <- nutrition_subset %>%
  distinct(PERSON_ID, MEASUREMENT_CONCEPT_ID) %>%
  count(MEASUREMENT_CONCEPT_ID, name = "n_patients") %>%
  arrange(desc(n_patients))

print(nutrition_patients_per_measurement)

# Count Patients Per Measurement ID and Site:
nutrition_patients_per_measurement_site <- nutrition_subset %>%
  distinct(PERSON_ID, MEASUREMENT_CONCEPT_ID, SRC_NAME) %>%
  count(MEASUREMENT_CONCEPT_ID, SRC_NAME, name = "n_patients") %>%
  arrange(MEASUREMENT_CONCEPT_ID, SRC_NAME)

print(nutrition_patients_per_measurement_site)


#--------check 4298279, 45889493, 45889491-------------

query_procedures <- "
SELECT 
  person_id, 
  procedure_concept_id, 
  procedure_datetime 
FROM omopcdm.procedure_occurrence
WHERE procedure_concept_id IN (4298279, 45889493, 45889491);
"
procedures_subset <- querySql(conn, query_procedures)

print(procedures_subset)


# Look in Observation table

query_observations <- "
SELECT 
  person_id, 
  observation_concept_id, 
  observation_datetime 
FROM omopcdm.observation
WHERE observation_concept_id IN (4298279, 45889493, 45889491);
"
observations_subset <- querySql(conn, query_observations)

print(observations_subset)


# Concept IDs Might Be Mapped Differently

query_mapping <- "
SELECT concept_id_1, concept_id_2, relationship_id
FROM omopcdm.concept_relationship
WHERE concept_id_1 IN (4298279, 45889493, 45889491);
"
mapping_results <- querySql(conn, query_mapping)

print(mapping_results)

#Find Standard Concept IDs

# My original IDs might be non-standard concepts that need to be replaced with standard ones for querying.


query_standard_concepts <- "
SELECT concept_id, concept_name, standard_concept
FROM omopcdm.concept
WHERE concept_id IN (4298279, 45889493, 45889491);
"
standard_concepts <- querySql(conn, query_standard_concepts)

print(standard_concepts)

#Concept ID 4298279 ("Nutrition therapy") is a standard concept (S), so it should be directly queryable in procedure_occurrence.

# Concept IDs 45889491 ("Medical Nutrition Therapy Procedures") and 45889493 ("Medical nutrition therapy") are classification concepts (C). Classification concepts cannot be used for direct querying because they are organizational categories rather than recorded data entries. Instead, they subsumes (groups together) other concepts.


# Find concept IDs for 45889491 and 45889493 that can be queried

query_subsumed_concepts <- "
SELECT cr.concept_id_1 AS classification_id, 
       cr.concept_id_2 AS standard_id, 
       c.concept_name, 
       c.domain_id 
FROM omopcdm.concept_relationship cr
JOIN omopcdm.concept c ON cr.concept_id_2 = c.concept_id
WHERE cr.concept_id_1 IN (45889491, 45889493)
  AND cr.relationship_id = 'Subsumes';
"
subsumed_results <- querySql(conn, query_subsumed_concepts)

print(subsumed_results)


# Results = 


#CLASSIFICATION_ID STANDARD_ID
#1          45889491    45889493
#2          45889493     2314318
#3          45889493     2314319
#4          45889493     2314320
#CONCEPT_NAME
#1                                                                                                  Medical nutrition therapy
#2 Medical nutrition therapy; initial assessment and intervention, individual, face-to-face with the patient, each 15 minutes
#3      Medical nutrition therapy; re-assessment and intervention, individual, face-to-face with the patient, each 15 minutes
#4                                                Medical nutrition therapy; group (2 or more individual(s)), each 30 minutes
#DOMAIN_ID
#1 Procedure
#2 Procedure
#3 Procedure
#4 Procedure

# next step is to query the procedure_occurrence table using these IDs.

query_nutrition_procedures <- "
SELECT 
  person_id, 
  procedure_concept_id, 
  procedure_datetime
FROM omopcdm.procedure_occurrence
WHERE procedure_concept_id IN (2314318, 2314319, 2314320);
"

nutrition_procedure_subset <- querySql(conn, query_nutrition_procedures)

print(nutrition_procedure_subset)

# count unique patients per procedure:

nutrition_patients_per_procedure <- nutrition_procedure_subset %>%
  distinct(PERSON_ID, PROCEDURE_CONCEPT_ID) %>%
  count(PROCEDURE_CONCEPT_ID, name = "n_patients") %>%
  arrange(desc(n_patients))

print(nutrition_patients_per_procedure)



# Let's merge nutrition and procedure
# Step 1: Combine Data from Measurement and Procedure Tables

# We'll merge patients from both measurement (nutrition-related) and procedure_occurrence (nutrition therapy procedures) into a unified dataset.

# Count nutrition records in measurement table
nutrition_measurement_subset <- nutrition_subset %>%
  distinct(PERSON_ID, MEASUREMENT_CONCEPT_ID) %>%
  mutate(category = "Measurement")

# Count nutrition records in procedure table
nutrition_procedure_subset <- nutrition_procedure_subset %>%
  distinct(PERSON_ID, PROCEDURE_CONCEPT_ID) %>%
  rename(MEASUREMENT_CONCEPT_ID = PROCEDURE_CONCEPT_ID) %>%
  mutate(category = "Procedure")

# Combine both datasets
nutrition_combined <- bind_rows(nutrition_measurement_subset, nutrition_procedure_subset)

print(nutrition_combined)

# Step 2: Merge Nutrition with Glucose Check Counts

# Now, we'll join the nutrition dataset with your existing glucose_counts table so we can compare them.

glucose_nutrition_combined <- nutrition_combined %>%
  left_join(glucose_counts, by = "PERSON_ID")

print(glucose_nutrition_combined)

# Step 3: Graph Nutrition Measures & Procedures Against Glucose Checks

# To visualize how nutrition-related interventions correlate with glucose checks, we'll create a boxplot and scatter plot.

# 1. Boxplot: Distribution of Glucose Checks Per Nutrition Type

ggplot(glucose_nutrition_combined, aes(x = category, y = n_glucose_checks, fill = category)) +
  geom_boxplot() +
  labs(title = "Distribution of Glucose Checks per Nutrition Type",
       x = "Nutrition Type",
       y = "Number of Glucose Checks") +
  theme_minimal()

# Visualize how blood glucose levels change after nutrition interventions. Here's how you can structure your analysis:
#---------------------------------------------------------------------------------------------------------------------
# Steps to Create the Scatter Plot

# 1. Filter Data to include nutrition measurements/procedures and glucose readings.

# 2. Define Time Relationship by linking glucose readings to nutrition interventions (e.g., within 1–24 hours after an intervention).

# 3. Plot Blood Glucose vs. Time Since Nutrition Intervention to track glucose level changes.

# Step 1: Prepare the Data

# We'll first identify glucose measurements recorded after a nutrition intervention.

glucose_after_nutrition <- measurement_subset %>%
  filter(MEASUREMENT_CONCEPT_ID %in% c(3004501, 3000483, 3031266, 3034962, 44816672, 3044242)) %>%
  left_join(nutrition_combined, by = "PERSON_ID") %>%
  mutate(
    time_since_nutrition = as.numeric(difftime(MEASUREMENT_DATETIME, VISIT_START_DATETIME, units = "hours"))
  ) %>%
  filter(time_since_nutrition > 0 & time_since_nutrition <= 24)  # Limit to first 24 hours


# Step 2: Create the Scatter Plot

# Now, we'll plot glucose levels against time since intervention:

ggplot(glucose_after_nutrition, aes(x = time_since_nutrition, y = VALUE_AS_NUMBER, color = category)) +
  geom_point(alpha = 0.6) +
  geom_smooth(method = "loess", se = FALSE) +  # Optional trend line
  labs(
    title = "Blood Glucose Levels After Nutrition Intervention",
    x = "Hours Since Nutrition Intervention",
    y = "Blood Glucose Level (mg/dL)"
  ) +
  theme_minimal()

# Figure is too busy. Let's focus on hypoglycemic patients 

# Step 1: Filter for Hypoglycemic Patients

# Only include glucose measurements where VALUE_AS_NUMBER < 70 and check them within 24 hours of nutrition intervention.

hypoglycemic_after_nutrition <- glucose_after_nutrition %>%
  filter(VALUE_AS_NUMBER < 70)

# Step 2: Generate a Scatter Plot
# Remove na
glucose_nutrition_combined <- glucose_nutrition_combined %>%
  filter(!is.na(category))


# Plot showing blood glucose levels over time after nutrition intervention for hypoglycemic patients.

ggplot(hypoglycemic_after_nutrition, aes(x = time_since_nutrition, y = VALUE_AS_NUMBER, color = category)) +
  geom_point(alpha = 0.7, size = 2) +
  geom_smooth(method = "loess", se = FALSE, color = "black") +  # Trendline
  labs(
    title = "Blood Glucose Levels After Nutrition Intervention (Hypoglycemic Patients)",
    x = "Hours Since Nutrition Intervention",
    y = "Blood Glucose Level (mg/dL)"
  ) +
  theme_minimal()


#---missing nutrition data
glucose_nutrition_combined %>%
  filter(is.na(category)) %>%
  select(PERSON_ID) %>%
  print()

levels(glucose_nutrition_combined$category)


# Measurement datetime and visit start datetime
glucose_nutrition_combined <- nutrition_combined %>%
  left_join(glucose_counts, by = "PERSON_ID") %>%
  left_join(measurement_subset %>% select(PERSON_ID, MEASUREMENT_DATETIME, VISIT_START_DATETIME), by = "PERSON_ID")

# Value as number addition
nutrition_measurement_only <- nutrition_measurement_only %>%
  left_join(measurement_subset %>% select(PERSON_ID, MEASUREMENT_CONCEPT_ID, VALUE_AS_NUMBER), 
            by = c("PERSON_ID", "MEASUREMENT_CONCEPT_ID"))


# Nutrition measurement

nutrition_measurement_only <- glucose_nutrition_combined %>%
  filter(category == "Measurement") %>%
  mutate(
    time_since_nutrition = as.numeric(difftime(MEASUREMENT_DATETIME, VISIT_START_DATETIME, units = "hours"))
  )



# Verify Additions
colnames(nutrition_measurement_only)
summary(nutrition_measurement_only$MEASUREMENT_DATETIME)
summary(nutrition_measurement_only$VISIT_START_DATETIME)

# Remove NA values, handling missing time
nutrition_measurement_only <- nutrition_measurement_only %>%
  filter(!is.na(MEASUREMENT_DATETIME))


# Create Scatter Plot

ggplot(nutrition_measurement_only, aes(x = time_since_nutrition, y = VALUE_AS_NUMBER, color = MEASUREMENT_CONCEPT_ID)) +
  geom_point(alpha = 0.7, size = 2) +
  geom_smooth(method = "loess", se = FALSE, color = "black") +  # Trendline
  labs(
    title = "Blood Glucose Levels After Nutrition Measurements",
    x = "Hours Since Nutrition Measurement",
    y = "Blood Glucose Level (mg/dL)"
  ) +
  theme_minimal()