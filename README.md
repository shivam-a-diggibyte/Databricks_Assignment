# Databricks Assignment

## Overview

This repository contains Databricks notebooks for an employee data ETL pipeline and API-based user ingestion. The work is organized into two assignment sections.

## Technologies Used

- Databricks Free Edition
- Python
- PySpark and Spark DataFrames
- Delta Lake
- Unity Catalog
- REST API (`reqres.in`)

## Repository Structure

```text
Databricks_Assignment/
├── README.md
├── Question_1/
│   ├── source_to_bronze/
│   │   ├── utils
│   │   └── employee_source_to_bronze
│   ├── bronze_to_silver/
│   │   └── employee_bronze_to_silver
│   └── silver_to_gold/
│       └── employee_silver_to_gold
└── Question_2/
    └── api_source_to_silver
```

> The names above represent notebook names. Add the appropriate notebook file extensions if you export the notebooks as source files.

## Question 1: Employee ETL Pipeline

The pipeline is divided into three stages:

1. **Source to Bronze**
   - Read Employee, Department, and Country CSV files.
   - Write the source data to Bronze output locations.

2. **Bronze to Silver**
   - Apply custom schemas while reading data.
   - Convert column names from camelCase to snake_case.
   - Add a `load_date` column.
   - Store the employee dimension as a Delta table.

3. **Silver to Gold**
   - Calculate salary totals by department in descending order.
   - Count employees by department and country.
   - Return department names with corresponding country names.
   - Calculate average age by department.
   - Add an `at_load_date` column and create a final employee fact table.

### Question 1 Output Tables

The current implementation uses Unity Catalog managed Delta tables:

- `workspace.employee_info.dim_employee`
- `workspace.employee_gold.department_salary`
- `workspace.employee_gold.employee_count`
- `workspace.employee_gold.department_country`
- `workspace.employee_gold.average_age`
- `workspace.employee_gold.fact_employee`

## Question 2: API Data Ingestion

The API ingestion notebook retrieves user data from `https://reqres.in/api/users` using pagination.

The notebook:

- Requests pages until the API returns an empty `data` array.
- Uses a custom PySpark schema.
- Creates a DataFrame from the user records.
- Excludes pagination metadata and the `support` block by selecting only the `data` records.
- Adds `site_address` with the value `reqres.in`.
- Adds a `load_date` column.
- Writes the result to a Delta table.

### Question 2 Output Table

- `workspace.site_info.person_info`

## How to Run

1. Import or open the notebooks in your Databricks workspace.
2. Upload the required Question 1 CSV files to the location expected by the notebooks.
3. Update input paths and catalog/schema names if your workspace uses different names.
4. Run the Question 1 notebooks in this order:
   - `source_to_bronze/utils`
   - `source_to_bronze/employee_source_to_bronze`
   - `bronze_to_silver/employee_bronze_to_silver`
   - `silver_to_gold/employee_silver_to_gold`
5. Run the Question 2 notebook: `api_source_to_silver`.
6. Verify the output tables in Unity Catalog.

## Implementation Notes

- The current implementation saves output as Unity Catalog managed Delta tables rather than writing every output to the exact physical paths described in the assignment.
- Ensure that the input file paths and catalog/schema names match your Databricks environment.
- Do not commit credentials, access tokens, or sensitive data to GitHub.

## Author

Shivam Kumar
