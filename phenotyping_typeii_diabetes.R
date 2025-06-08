library(tidyverse)
library(magrittr)
library(bigrquery)
library(caret)
library(dplyr)
library(RPostgres)
library(DBI)

con <- dbConnect(
  bigrquery::bigquery(),
  project = "learnclinicaldatascience",
  dataset = "course3_data",
  billing = "learnclinicaldatascience"
)

diabetes <- tbl(con, "diabetes_goldstandard")

diabetes

#===========================================

# Creating Training and Testing Populations

#===========================================

#Let’s do a roughly 80/20 split. We will put 80 records in the training population 
# and the remaining 19 records will be our testing population.

training <- diabetes %>% 
  collect() %>% 
  sample_n(80)


# To create our testing population we can just invert this list.

testing <- diabetes %>% 
  filter(!SUBJECT_ID %in% training$SUBJECT_ID)

#Load training data
training <- tbl(con, "course3_data.diabetes_training")

#2x2 confusion matrix 

## getStats(df, predicted, reference)
getStats <- function(df, ...){
  df %>%
    select_(.dots = lazyeval::lazy_dots(...)) %>%
    mutate_all(funs(factor(., levels = c(1,0)))) %>% 
    table() %>% 
    confusionMatrix()
}
df

#====================================

# ICD Codes for Diabetes II Mellitus 

#====================================

# icd code 250.00 Type II diabetes mellitus without complications, not uncontrolled


# icd codes
diagnoses_icd <- tbl(con, "mimic3_demo.DIAGNOSES_ICD")

icd_25000 <- diagnoses_icd %>% 
  filter(ICD9_CODE == "25000") %>% 
  distinct(SUBJECT_ID) %>% 
  mutate(icd_25000 = 1)

icd_25000

# We then can join icd_25000 with the diabetes data frame that contains the record of our manual review results.

training %>% 
  left_join(icd_25000)

training %>% 
  left_join(icd_25000) %>% 
  mutate(icd_25000 = coalesce(icd_25000, 0))

# two columns each where 1 is positive and 0 is negative.
# Let’s save these results to the diabetes data frame, we can use the %<>% pipe to do this in a single step.

training %<>% 
  left_join(icd_25000) %>% 
  mutate(icd_25000 = coalesce(icd_25000, 0))

# And now we can use our getStats() function to calculate the performance of icd_25000:
training %>% 
  collect() %>% 
  getStats(icd_25000, DIABETES)

#--------Now let's view icd 250.02---------------------
# icd 250.02 = Diabetes mellitus without mention of complication, type II or unspecified type, uncontrolled

# icd codes
diagnoses_icd <- tbl(con, "mimic3_demo.DIAGNOSES_ICD")

# icd 250.02

icd_25002 <- diagnoses_icd %>% 
  filter(ICD9_CODE == "25002") %>% 
  distinct(SUBJECT_ID) %>% 
  mutate(icd_25002 = 1)

icd_25002

# We then can join icd_250.02 with the diabetes data frame that contains the record of our manual review results.

training %>% 
  left_join(icd_25002)

training %>% 
  left_join(icd_25002) %>% 
  mutate(icd_25002 = coalesce(icd_25002, 0))

# two columns each where 1 is positive and 0 is negative.
# Let’s save these results to the diabetes data frame, we can use the %<>% pipe to do this in a single step.

training %<>% 
  left_join(icd_25002) %>% 
  mutate(icd_25002 = coalesce(icd_25002, 0))

# And now we can use our getStats() function to calculate the performance of icd_250.02:
training %>% 
  collect() %>% 
  getStats(icd_25002, DIABETES)


#==================================

# Investigating related Lab events

#==================================


#-------=---------------------------HbA1c-------------------------------
library(DBI)

query <- "SELECT * FROM mimic3_demo.D_LABITEMS WHERE LOWER(LABEL) LIKE '%a1c%'"
results <- dbGetQuery(con, query)

results

# Because HbA1C is used to diagnose diabetes let’s see how ever having the test, 
#regardless of the test value, performs in identifying our diabetics.

labevents <- tbl(con, "mimic3_demo.LABEVENTS")

hba1c <- labevents %>% 
  filter(ITEMID %in% c(50852,50854)) %>% 
  distinct(SUBJECT_ID) %>% 
  mutate(hba1c = 1)

training %<>% 
  left_join(hba1c) %>% 
  mutate(hba1c = coalesce(hba1c, 0))

training %>% 
  collect() %>% 
  getStats(hba1c, DIABETES)


#----------------LABEL is Glucose, FLUID is Blood, and CATEGORY is Blood Gas---------------

query <- "
SELECT * 
FROM mimic3_demo.D_LABITEMS 
WHERE LOWER(LABEL) = 'glucose'
  AND LOWER(FLUID) = 'blood'
  AND LOWER(CATEGORY) = 'blood gas'
"
results <- dbGetQuery(con, query)

results


#id glucose, blood and blood gas in diabetics
labevents <- tbl(con, "mimic3_demo.LABEVENTS")

glucose_blood_gas <- labevents %>% 
  filter(ITEMID %in% c(50809)) %>% 
  distinct(SUBJECT_ID) %>% 
  mutate(glucose_blood_gas = 1)

training %<>% 
  left_join(glucose_blood_gas) %>% 
  mutate(glucose_blood_gas = coalesce(glucose_blood_gas, 0))

training %>% 
  collect() %>% 
  getStats(glucose_blood_gas, DIABETES)


#--------------------------------Metformin------------------------------------------------

prescriptions <- tbl(con, "mimic3_demo.PRESCRIPTIONS")

metformin <- prescriptions %>% 
  filter(tolower(DRUG) %like% "%metformin%") %>% 
  distinct(SUBJECT_ID) %>% 
  mutate(metformin = 1)

training %<>% 
  left_join(metformin) %>% 
  mutate(metformin = coalesce(metformin, 0))

training %>% 
  collect() %>% 
  getStats(metformin, DIABETES)



#--------------------------------Glipizide------------------------------------------------

query4 <- "
SELECT * 
FROM mimic3_demo.PRESCRIPTIONS 
WHERE LOWER(DRUG) LIKE '%glipizide%'
"
results <- dbGetQuery(con, query4)
results

# From my working query ----view diabetic patients on glipizide
query4 <- "
  SELECT DISTINCT SUBJECT_ID
  FROM mimic3_demo.PRESCRIPTIONS 
  WHERE LOWER(DRUG) LIKE '%glipizide%'
"
glipizide_subjects <- dbGetQuery(con, query4) %>%
  mutate(glipizide = 1)

glipizide_subjects

glipizide <- prescriptions %>% 
  filter(tolower(DRUG) %like% "%glipizide%") %>% 
  distinct(SUBJECT_ID) %>% 
  mutate(glipizide = 1)
training %>% 
  left_join(glipizide) %>% 
  mutate(glipizide = coalesce(glipizide,0)) %>% 
  collect() %>% 
  getStats(glipizide, DIABETES)




