### SumUp Case Study
This repository contains the submitted technical solution to the provided case study. For anonymity purposes, the sample data is not uploaded. 

## Task description
Create an end to end ETL pipeline using the provided sample data with SQL & DBT models that would answer the following questions:
1. Which acquisition channel performed the best in each country in 2023?
2. Which acquisition channel performed the worst in each country in 2023?
3. Who were the top 3 best performing sales reps in 2023?
4. How long was the average lead to live cycle, overall and by top 5 typologies?

## Technology used and architecture
For this task I decided to go with a modern tech stack approach of: **Snowflake** for data processing and **DBT** for orchestration. The models are theoretically  ready to be deployed in CI/CD DBT infrastructure.
Note: for time purposes, .yml files are not complete but contain only example metadata. 

# Architecture
I have decided to go with a common "bronze - silver - golden"like layer architecture with following structure: 
1. (Bronze) ODS (Operational Data Store) layer - contains cleaned data with standardized explicit column names
2. (Silver) Core layer - main, multi-purpose layer. Contains harmonized data structures that can be used for multiple purposes aimed at a specific business case and incorporates business terminology. In the Core layer, the data structured in dimensional and fact (metric) models that can be joined with a common identifier (Primary & Foreign Keys). 
3. (Gold) Reporting layer - reporting purpose layer. Ready to be used data models that can be imported into a BI tool or to be queried for particular ad-hoc purposes. In other words, in this layer one can find the outcome of the task.

The lineage of the models is described in the picture below:
<img width="893" alt="image" src="https://github.com/user-attachments/assets/afa1c15a-44f1-4270-9eb4-10c67a4c4186">

The name of the tables corresponds  to the model name. 

## Data assumptions
During the creation of the models, several assumptions have been made:
1. Data coming from raw_financials is the closest(source) to the truth and the most complete. This assumption was made due to the fact that in case of an external audit, financial data is reviewed. Also, usually these datapoints are shared with investors and banks. 
2. In case of duplicated data points e.g. one customer having 2 different acquisition dates, the earliest date is used (when the customer appeared for the first time in the systems)
3. In case of missing data points for customers, the store data (coming from raw_store) should be used (backfilling the missing datapoints)
4. For seller and typology performance, total 5 year lifetime value is used instead of LTV/CAC as the respective dimensions do not (usually) affect the costs

## Task outcomes:
#1. Which acquisition channel performed the best in each country in 2023? Which acquisition channel performed the worst in each country in 2023?

<img width="924" alt="image" src="https://github.com/user-attachments/assets/cc103477-b63d-4846-abc2-0b7d7e73d54a">

#2. Who were the top 3 best performing sales reps in 2023?

<img width="924" alt="image" src="https://github.com/user-attachments/assets/7a844b49-e5e0-47e2-b12a-19fba3c91e80">

#3. How long was the average lead to live cycle, overall and by top 5 typologies?

<img width="908" alt="image" src="https://github.com/user-attachments/assets/de5a3e46-2fb8-4823-ba76-3b9b6adbdc4c">
