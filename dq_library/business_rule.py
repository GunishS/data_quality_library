from pyspark.sql.functions import col, lit, isnan
from pyspark.sql.types import *
from datetime import datetime
from functools import reduce

# ---- Data Quality class wrapper
class DataQuality:
    def __init__(self, catalog="dev", schema="silver_ods"):
        self.catalog = catalog
        self.schema = schema

    # ===============================================================
    # 1. Generic DQ checks (Nulls, Ranges, Referential Integrity)
    # ===============================================================
    def run_dq_checks_df(self, fact_df, fact_table_name, config):
        error_dfs = []
        df_schema = fact_df.schema

        null_check_enabled = config.get("enable_null_check", True)
        range_check_enabled = config.get("enable_range_check", True)
        referential_check_enabled = config.get("enable_referential_check", True)

        # ---------- Null Checks ----------
        null_violated_cols = []
        null_errors_count = 0
        if null_check_enabled:
            pk_cols = [c for c in config.get("primary_keys", []) if c in fact_df.columns]
            for col_name in pk_cols:
                if isinstance(df_schema[col_name].dataType, NumericType):
                    null_count = fact_df.filter(col(col_name).isNull() | isnan(col(col_name))).limit(1).count()
                else:
                    null_count = fact_df.filter(col(col_name).isNull()).limit(1).count()
                if null_count > 0:
                    null_violated_cols.append(col_name)
            if null_violated_cols:
                null_conds = [
                    (col(c).isNull() | isnan(col(c))) if isinstance(df_schema[c].dataType, NumericType)
                    else col(c).isNull()
                    for c in null_violated_cols
                ]
                null_errors_count = fact_df.filter(reduce(lambda a, b: a | b, null_conds)).count()
                null_errors = fact_df.filter(reduce(lambda a, b: a | b, null_conds)) \
                                     .withColumn("dq_error_type", lit("Null Check").cast(StringType()))
                error_dfs.append(null_errors)

        # ---------- Range Checks ----------
        range_errors_count = 0
        if range_check_enabled:
            try:
                range_filter = (
                    (col(config["date_column"]) < lit(config["date_min"])) |
                    (col(config["date_column"]) > lit(config["date_max"])) |
                    (col(config["range_column"]) < lit(config["range_min"])) |
                    (col(config["range_column"]) > lit(config["range_max"]))
                )
                range_errors_count = fact_df.filter(range_filter).count()
                if range_errors_count > 0:
                    range_errors = fact_df.filter(range_filter) \
                                          .withColumn("dq_error_type", lit("Range Check").cast(StringType()))
                    error_dfs.append(range_errors)
            except Exception as e:
                print(f"Range check failed: {e}")
                range_errors_count = 0

        # ---------- Referential Checks ----------
        referential_errors_count = 0
        referential_missing_values = []
        referential_issues = []
        if referential_check_enabled:
            for fk in config.get("foreign_keys", []):
                fk_col = fk["fk_column"]
                dim_table = fk["dim_table"]
                dim_fk_col = fk.get("dim_fk_column", fk_col)
                try:
                    dim_df = spark.table(dim_table)
                    missing_df = fact_df.select(fk_col).distinct().na.drop() \
                                        .join(dim_df.select(dim_fk_col).distinct().na.drop(),
                                              on=fact_df[fk_col] == dim_df[dim_fk_col],
                                              how="left_anti")
                    count = missing_df.count()
                    if count > 0:
                        samples = [r[fk_col] for r in missing_df.limit(5).collect()]
                        referential_missing_values.extend(samples)
                        referential_issues.append(f"{fk_col} → {dim_table} ({count} missing, e.g. {samples})")
                        referential_errors_count += fact_df.filter(col(fk_col).isin(samples)).count()
                        ref_errors = fact_df.filter(col(fk_col).isin(samples)) \
                                            .withColumn("dq_error_type", lit(f"Referential Check on {fk_col}").cast(StringType()))
                        error_dfs.append(ref_errors)
                except Exception as e:
                    referential_issues.append(f"{fk_col} → {dim_table} (error: {str(e)})")

        # ---------- Write Error Records ----------
        error_df = None
        if error_dfs:
            error_df = reduce(lambda a, b: a.unionByName(b), error_dfs).dropDuplicates()
            error_tbl = f"{self.catalog}.{self.schema}.{fact_table_name.split('.')[-1]}_error"
            write_mode = "append" if spark.catalog.tableExists(error_tbl) else "overwrite"
            error_df.write.mode(write_mode).format("delta").option("mergeSchema", "true").saveAsTable(error_tbl)
            print(f"Error records {'appended to' if write_mode == 'append' else 'written to'} {error_tbl}")
        else:
            print("No error records to write in run_dq_checks_df.")

        # ---------- Write Summary ----------
        total_error_records = null_errors_count + range_errors_count + referential_errors_count
        summary_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("null_check_enabled", BooleanType(), True),
            StructField("null_error_count", IntegerType(), True),
            StructField("range_check_enabled", BooleanType(), True),
            StructField("range_error_count", IntegerType(), True),
            StructField("referential_check_enabled", BooleanType(), True),
            StructField("referential_error_count", IntegerType(), True),
            StructField("total_records", LongType(), True),
            StructField("total_error_records", LongType(), True),
            StructField("testing_ts", TimestampType(), True),
        ])
        summary_data = [{
            "table_name": fact_table_name,
            "null_check_enabled": null_check_enabled,
            "null_error_count": int(null_errors_count),
            "range_check_enabled": range_check_enabled,
            "range_error_count": int(range_errors_count),
            "referential_check_enabled": referential_check_enabled,
            "referential_error_count": int(referential_errors_count),
            "total_records": int(fact_df.count()),
            "total_error_records": int(total_error_records),
            "testing_ts": datetime.now()
        }]
        summary_df = spark.createDataFrame(summary_data, schema=summary_schema)
        summary_df.write.mode("append").format("delta").saveAsTable(f"{self.catalog}.{self.schema}.dq_check")

        # ---------- Return Good Records ----------
        if error_df:
            good_df = fact_df.subtract(error_df.drop("dq_error_type"))
            return good_df
        else:
            return fact_df

    # ===============================================================
    # 2. Unrealistic Quantity Check (IQR or min/max)
    # ===============================================================
    def unrealistic_quantity(self, fact_df, fact_table_name, config_business_rule):
        error_dfs = []
        range_errors_count = 0

        try:
            for column in config_business_rule.get("columns", []):
                col_name = column["name"]

                # Case 1: IQR-based
                if column["min_value"] == "IQR" or column["max_value"] == "IQR":
                    values = fact_df.select(col(col_name)).na.drop().rdd.map(lambda r: r[0]).collect()
                    if not values:
                        continue
                    iqrobj = median(values)
                    q1, q2, q3 = iqrobj.calculate_iqr()

                    cond = (col(col_name) < q1) | (col(col_name) > q3)
                    count = fact_df.filter(cond).count()
                    range_errors_count += count

                    if count > 0:
                        error_subset = fact_df.filter(cond) \
                                              .withColumn("dq_error_type", lit("IQR Check").cast(StringType())) \
                                              .withColumn("dq_error_code", lit(config_business_rule["error_code"])) \
                                              .withColumn("dq_error_message", lit(config_business_rule["error_message"].format(
                                                          table_name=config_business_rule["table_name"],
                                                          column=col_name)))
                        error_dfs.append(error_subset)

                # Case 2: Fixed Range
                else:
                    min_val = column["min_value"]
                    max_val = column["max_value"]

                    cond = (col(col_name) < min_val) | (col(col_name) > max_val)
                    count = fact_df.filter(cond).count()
                    range_errors_count += count

                    if count > 0:
                        error_subset = fact_df.filter(cond) \
                                              .withColumn("dq_error_type", lit("Range Check").cast(StringType())) \
                                              .withColumn("dq_error_code", lit(config_business_rule["error_code"])) \
                                              .withColumn("dq_error_message", lit(config_business_rule["error_message"].format(
                                                          table_name=config_business_rule["table_name"],
                                                          column=col_name)))
                        error_dfs.append(error_subset)

        except Exception as e:
            print(f"Unrealistic quantity check failed: {e}")

        # ---------- Write Error Records ----------
        error_df = None
        if error_dfs:
            error_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), error_dfs).dropDuplicates()
            error_tbl = f"{self.catalog}.{self.schema}.{fact_table_name.split('.')[-1]}_error"
            write_mode = "append" if spark.catalog.tableExists(error_tbl) else "overwrite"
            error_df.write.mode(write_mode).format("delta").option("mergeSchema", "true").saveAsTable(error_tbl)
            print(f"Error records {'appended to' if write_mode == 'append' else 'written to'} {error_tbl}")
        else:
            print("No error records to write for unrealistic quantity check.")

        # ---------- Write Summary ----------
        summary_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("rule_type", StringType(), True),
            StructField("error_count", IntegerType(), True),
            StructField("total_records", LongType(), True),
            StructField("testing_ts", TimestampType(), True),
        ])
        summary_data = [{
            "table_name": fact_table_name,
            "rule_type": "Unrealistic Quantity",
            "error_count": int(range_errors_count),
            "total_records": int(fact_df.count()),
            "testing_ts": datetime.now()
        }]
        summary_df = spark.createDataFrame(summary_data, schema=summary_schema)
        summary_df.write.mode("append").format("delta").saveAsTable(f"{self.catalog}.{self.schema}.dq_check")

        # ---------- Return Good Records ----------
        if error_df:
            good_df = fact_df.subtract(error_df.drop("dq_error_type", "dq_error_code", "dq_error_message"))
            return good_df
        else:
            return fact_df

    def run_dq_checks_vw(self, fact_table_name, config):
        """
        Run data quality checks on a Spark view/table specified by `fact_table_name`.
        Error records are saved to <table_name>_error and summary to dq_check.
        Returns a DataFrame of good records.
        """
        # Load Spark table/view
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

        # Combine error records and write
        error_df = None
        if error_dfs:
            error_df = reduce(lambda a, b: a.unionByName(b), error_dfs).dropDuplicates()
            error_tbl = f"{self.catalog}.{self.schema}.{fact_table_name.split('.')[-1]}_error"
            write_mode = "append" if spark.catalog.tableExists(error_tbl) else "overwrite"
            error_df.write.mode(write_mode).format("delta").option("mergeSchema", "true").saveAsTable(error_tbl)
            print(f"Error records {'appended to' if write_mode == 'append' else 'written to'} {error_tbl}")
        else:
            print("No error records to write.")

        # ================== 5. Append Summary to dq_check ==================
        null_errors_count = 0
        range_errors_count = 0
        referential_errors_count = 0

        if null_violated_cols:
            null_conds = [(col(c).isNull() | isnan(col(c))) if isinstance(schema[c].dataType, NumericType)
                          else col(c).isNull() for c in null_violated_cols]
            null_errors_count = fact_df.filter(reduce(lambda a, b: a | b, null_conds)).count()

        if config.get("enable_range_check", True) and range_filter is not None:
            range_errors_count = fact_df.filter(range_filter).count()

        if not referential_check_passed and referential_missing_values:
            for fk in config.get("foreign_keys", []):
                fk_col = fk["fk_column"]
                vals = [v for v in referential_missing_values if v is not None]
                if fk_col in fact_df.columns:
                    referential_errors_count += fact_df.filter(col(fk_col).isin(vals)).count()

        summary_df = spark.createDataFrame([{
            "table_name": fact_table_name,
            "null_check_passed": null_check_passed,
            "null_violations": null_violation_result,
            "null_errors_count": null_errors_count,
            "range_violations": range_violation_count,
            "range_errors_count": range_errors_count,
            "referential_check_passed": referential_check_passed,
            "referential_issues": referential_status,
            "referential_errors_count": referential_errors_count,
            "total_records": fact_df.count(),
            "testing_ts": datetime.now()
        }])
        summary_df.write.mode("append").format("delta").saveAsTable(f"{self.catalog}.{self.schema}.dq_check")

        # ================== 6. Return Good Records ==================
        if error_df:
            good_df = fact_df.subtract(error_df.drop("dq_error_type"))
            return good_df
        else:
            return fact_df



# ====================================================

dq = DataQuality(catalog="dev", schema="silver_ods")

# Run generic dq checks
good_df_1 = dq.run_dq_checks_df(fact_df, "invoice_line_item", config)

# Run unrealistic quantity dq check
good_df_2 = dq.unrealistic_quantity(fact_df, "invoice_line_item", config_business_rule)