#  dq_library

A lightweight, reusable **Data Quality (DQ) check library** for different layers that can be intregated and used with multiple artifacts like data pipeline, notebook etc.

## 🚀 Features

🧪 How the Data Quality Library Works
This library is designed to make it easy to run data quality (DQ) checks on any Spark DataFrame, SQL View, or table within a data pipeline. The workflow is made up of four major steps that ensure invalid records are isolated, audits are logged, and only high-quality data is passed forward.

## command to run/call
from dq_library.config import config as dq_config
good_df = run_dq_checks(fact_df, fact_table_name, config=dq_config)


🔷 1. Run Data Quality Checks
Use either run_dq_checks_df() or run_dq_checks_vw() to perform DQ checks on a DataFrame or a registered View/Table:

run_dq_checks_df: For running DQ checks directly on a PySpark DataFrame.
run_dq_checks_vw: For running DQ checks on a registered Spark SQL view or a table name (string).
These functions apply user-defined rules via the config dictionary to validate:

Null Checks on primary/essential columns.
Range Checks for defined numeric or date columns.
Referential Integrity Checks across foreign key relationships.

🧾 2. Error Table Creation
For every fact table passed into the DQ function, an error Delta table is automatically generated to store failed records.

📌 Naming convention:
{table_name}_error

This table helps you analyze and debug data issues afterward. It includes the original columns along with a dq_error_type column explaining the validation failure (e.g., "Null Check", "Range Check").

📊 3. Data Quality Summary Logging
Each time the DQ job runs, a high-level summary record is appended to a centralized audit table:
{catalog}.{schema}.dq_check

The log includes:

Table tested
Null check result
Range violation counts
Referential check status
Timestamp of execution
Total records processed
This enables end-to-end auditability and serves as a historical tracking table for DQ runs.

✅ 4. Return Clean/Good Records
Finally, the process subtracts all invalid records (those failing any rule) and returns a new DataFrame containing only valid/good records.

This clean DataFrame can then be forwarded downstream for analytics, ML training, reporting, or transformation—in full confidence of its quality.
---

## 🔧 Installation

Install directly using pip:
pip install dq-library


