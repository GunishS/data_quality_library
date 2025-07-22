# ======================================= v3 =============================================
# ============ 1. run_dq_checks_df or run_dq_checks_vw funtion to run check on df/view/tbl
# ============ 2. Error tbls (<table_name>_error) to be created for each tbl =============
# ============ 3. Append test result of each tbl to dq_check tbl  ========================
# ============ 4. Return Good Records ====================================================

from functools import reduce
from pyspark.sql.functions import col, lit, isnan
from pyspark.sql.types import NumericType
from datetime import datetime
import pyspark.sql.functions as F

path_for_error_tbl = "dev.silver_ods"

def run_dq_checks_df(fact_df, fact_table_name, config):
    error_dfs = []
    schema = fact_df.schema

    # ================== 1. Null Checks ==================
    null_check_passed = True
    null_violation_result = "Null check skipped"
    null_violated_cols = []

    if config.get("enable_null_check", True):
        pk_cols = [c for c in config.get("primary_keys", []) if c in fact_df.columns]
        for col_name in pk_cols:
            if isinstance(schema[col_name].dataType, NumericType):
                null_count = fact_df.filter(col(col_name).isNull() | isnan(col(col_name))).limit(1).count()
            else:
                null_count = fact_df.filter(col(col_name).isNull()).limit(1).count()
            if null_count > 0:
                null_violated_cols.append(col_name)

        null_check_passed = len(null_violated_cols) == 0
        null_violation_result = "All primary keys passed" if null_check_passed else ", ".join(null_violated_cols)

    # ================== 2. Range Checks ==================
    range_violation_count = 0
    range_check_passed = True
    range_filter = None

    if config.get("enable_range_check", True):
        try:
            range_filter = (
                (col(config["date_column"]) < lit(config["date_min"])) |
                (col(config["date_column"]) > lit(config["date_max"])) |
                (col(config["range_column"]) < lit(config["range_min"])) |
                (col(config["range_column"]) > lit(config["range_max"]))
            )
            range_violation_count = fact_df.filter(range_filter).count()
            range_check_passed = range_violation_count == 0
        except Exception as e:
            print(f" Range check failed: {e}")
            range_check_passed = False

    # ================== 3. Referential Checks ==================
    referential_issues = []
    referential_missing_values = []
    referential_check_passed = True
    referential_status = "Referential check skipped"

    if config.get("enable_referential_check", True):
        for fk in config.get("foreign_keys", []):
            fk_col = fk["fk_column"]
            dim_table = fk["dim_table"]
            dim_fk_col = fk.get("dim_fk_column", fk_col)

            try:
                dim_df = spark.table(dim_table)
                missing_df = fact_df.select(fk_col).distinct().na.drop().join(
                    dim_df.select(dim_fk_col).distinct().na.drop(),
                    on=fact_df[fk_col] == dim_df[dim_fk_col],
                    how="left_anti"
                )
                count = missing_df.count()
                if count > 0:
                    samples = [r[fk_col] for r in missing_df.limit(5).collect()]
                    referential_missing_values.extend(samples)
                    referential_issues.append(f"{fk_col} → {dim_table} ({count} missing, e.g. {samples})")
            except Exception as e:
                referential_issues.append(f"{fk_col} → {dim_table} (error: {str(e)})")

        referential_check_passed = len(referential_issues) == 0
        referential_status = "All passed" if referential_check_passed else "; ".join(referential_issues)

    # ================== 4. Build Error DataFrame ==================
    if null_violated_cols:
        null_conds = [
            (col(c).isNull() | isnan(col(c))) if isinstance(schema[c].dataType, NumericType)
            else col(c).isNull() for c in null_violated_cols
        ]
        null_errors = fact_df.filter(reduce(lambda a, b: a | b, null_conds)) \
                             .withColumn("dq_error_type", lit("Null Check"))
        error_dfs.append(null_errors)

    if not range_check_passed and config.get("enable_range_check", True) and range_filter is not None:
        range_errors = fact_df.filter(range_filter).withColumn("dq_error_type", lit("Range Check"))
        error_dfs.append(range_errors)

    if not referential_check_passed and referential_missing_values:
        for fk in config.get("foreign_keys", []):
            fk_col = fk["fk_column"]
            vals = [v for v in referential_missing_values if v is not None]
            if fk_col in fact_df.columns:
                ref_errors = fact_df.filter(col(fk_col).isin(vals)) \
                                    .withColumn("dq_error_type", lit(f"Referential Check on {fk_col}"))
                error_dfs.append(ref_errors)

    # Combine to write error records
    error_df = None
    if error_dfs:
        error_df = reduce(lambda a, b: a.unionByName(b), error_dfs).dropDuplicates()
        error_tbl = f"dev.silver_ods.{fact_table_name.split('.')[-1]}_error"

        if spark.catalog.tableExists(error_tbl):
            write_mode = "append"
        else:
            write_mode = "overwrite"

        # Write error records
        error_df.write \
            .mode(write_mode) \
            .format("delta") \
            .option("mergeSchema", "true") \
            .saveAsTable(error_tbl)

        print(f"Error records {'appended to' if write_mode == 'append' else 'written to'} {error_tbl}")
    else:
        print(" No error records to write.")


    # ================== 5. Append Summary to dq_check ==================
    summary_df = spark.createDataFrame([{
        "fact_table": fact_table_name,
        # "missing_fact_keys_sample": str(referential_missing_values),
        "null_check_passed": null_check_passed,
        "null_violations": null_violation_result,
        "range_violations": range_violation_count,
        "referential_check_passed": referential_check_passed,
        "referential_issues": referential_status,
        "total_records": fact_df.count(),
        "testing_ts": datetime.now()
    }])

    summary_df.write.mode("append").format("delta").saveAsTable("dev.silver_ods.dq_check")

    # ================== 6. Return Good Records ==================
    if error_df:
        good_df = fact_df.subtract(error_df.drop("dq_error_type"))
        return good_df
    else:
        return fact_df




def run_dq_checks_vw(fact_table_name, config):
    """
    Run data quality checks on a Spark view/table specified by `fact_table_name`.
    Error records are saved to <table_name>_error and summary to dev.silver_ods.dq_check.
    Returns a DataFrame of good records (i.e., passing all DQ checks).
    """
    # ==== Load the Spark view/tbl as DataFrame
    fact_df = spark.table(fact_table_name)
    error_dfs = []
    schema = fact_df.schema

    # ================== 1. Null Checks ==================
    null_check_passed = True
    null_violation_result = "Null check skipped"
    null_violated_cols = []

    if config.get("enable_null_check", True):
        pk_cols = [c for c in config.get("primary_keys", []) if c in fact_df.columns]
        for col_name in pk_cols:
            if isinstance(schema[col_name].dataType, NumericType):
                null_count = fact_df.filter(col(col_name).isNull() | isnan(col(col_name))).limit(1).count()
            else:
                null_count = fact_df.filter(col(col_name).isNull()).limit(1).count()
            if null_count > 0:
                null_violated_cols.append(col_name)

        null_check_passed = len(null_violated_cols) == 0
        null_violation_result = "All primary keys passed" if null_check_passed else ", ".join(null_violated_cols)

    # ================== 2. Range Checks ==================
    range_violation_count = 0
    range_check_passed = True
    range_filter = None

    if config.get("enable_range_check", True):
        try:
            range_filter = (
                (col(config["date_column"]) < lit(config["date_min"])) |
                (col(config["date_column"]) > lit(config["date_max"])) |
                (col(config["range_column"]) < lit(config["range_min"])) |
                (col(config["range_column"]) > lit(config["range_max"]))
            )
            range_violation_count = fact_df.filter(range_filter).count()
            range_check_passed = range_violation_count == 0
        except Exception as e:
            print(f"Range check failed: {e}")
            range_check_passed = False

    # ================== 3. Referential Checks ==================
    referential_issues = []
    referential_missing_values = []
    referential_check_passed = True
    referential_status = "Referential check skipped"

    if config.get("enable_referential_check", True):
        for fk in config.get("foreign_keys", []):
            fk_col = fk["fk_column"]
            dim_table = fk["dim_table"]
            dim_fk_col = fk.get("dim_fk_column", fk_col)

            try:
                dim_df = spark.table(dim_table)
                missing_df = fact_df.select(fk_col).distinct().na.drop().join(
                    dim_df.select(dim_fk_col).distinct().na.drop(),
                    on=fact_df[fk_col] == dim_df[dim_fk_col],
                    how="left_anti"
                )
                count = missing_df.count()
                if count > 0:
                    samples = [r[fk_col] for r in missing_df.limit(5).collect()]
                    referential_missing_values.extend(samples)
                    referential_issues.append(f"{fk_col} → {dim_table} ({count} missing, e.g. {samples})")
            except Exception as e:
                referential_issues.append(f"{fk_col} → {dim_table} (error: {str(e)})")

        referential_check_passed = len(referential_issues) == 0
        referential_status = "All passed" if referential_check_passed else "; ".join(referential_issues)

    # ================== 4. Build Error DataFrame ==================
    if null_violated_cols:
        null_conds = [
            (col(c).isNull() | isnan(col(c))) if isinstance(schema[c].dataType, NumericType)
            else col(c).isNull() for c in null_violated_cols
        ]
        null_errors = fact_df.filter(reduce(lambda a, b: a | b, null_conds)) \
                             .withColumn("dq_error_type", lit("Null Check"))
        error_dfs.append(null_errors)

    if not range_check_passed and config.get("enable_range_check", True) and range_filter is not None:
        range_errors = fact_df.filter(range_filter).withColumn("dq_error_type", lit("Range Check"))
        error_dfs.append(range_errors)

    if not referential_check_passed and referential_missing_values:
        for fk in config.get("foreign_keys", []):
            fk_col = fk["fk_column"]
            vals = [v for v in referential_missing_values if v is not None]
            if fk_col in fact_df.columns:
                ref_errors = fact_df.filter(col(fk_col).isin(vals)) \
                                    .withColumn("dq_error_type", lit(f"Referential Check on {fk_col}"))
                error_dfs.append(ref_errors)

    # Combine to write error records
    error_df = None
    if error_dfs:
        error_df = reduce(lambda a, b: a.unionByName(b), error_dfs).dropDuplicates()
        error_tbl = f"{path_for_error_tbl}.{fact_table_name.split('.')[-1]}_error"

        write_mode = "append" if spark.catalog.tableExists(error_tbl) else "overwrite"

        error_df.write \
            .mode(write_mode) \
            .format("delta") \
            .option("mergeSchema", "true") \
            .saveAsTable(error_tbl)

        print(f"Error records {'appended to' if write_mode == 'append' else 'written to'} {error_tbl}")
    else:
        print("No error records to write.")

    # ================== 5. Append Summary to dq_check ==================
    summary_df = spark.createDataFrame([{
        "fact_table": fact_table_name,
        "null_check_passed": null_check_passed,
        "null_violations": null_violation_result,
        "range_violations": range_violation_count,
        "referential_check_passed": referential_check_passed,
        "referential_issues": referential_status,
        "total_records": fact_df.count(),
        "testing_ts": datetime.now()
    }])

    summary_df.write.mode("append").format("delta").saveAsTable("dev.silver_ods.dq_check")

    # ================== 6. Return Good Records ==================
    if error_df:
        good_df = fact_df.subtract(error_df.drop("dq_error_type"))
        return good_df
    else:
        return fact_df

