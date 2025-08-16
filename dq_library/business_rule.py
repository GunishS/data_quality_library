from datetime import datetime
from pyspark.sql.functions import col, count, isnan, lit, when
from pyspark.sql.types import NumericType,LongType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
from functools import reduce
import pyspark.sql.functions as F
from .median import median
from pyspark.sql.types import *
from pyspark.sql.types import *



config_business_rule = {
    "table_name":"invoice_line_item",
    "columns":[
        {
            "name": "quantity",
            "min_value": "IQR",
            "max_value": "IQR" 
        },
        {
            "name": "order",
            "min_value": 1,
            "max_value": 100
        }
    ],
    "error_message":"Unrealistic quantity found in {table_name} for {column}",
    "error_code":"DQ001",
}
# 3:17pm

#  =================================================================

catalog = "dev"
schema = "silver_ods"

def unrealistic_quantity(fact_df, fact_table_name, config_business_rule):
    error_dfs = []
    range_errors_count = 0

    try:
        for column in config_business_rule.get("columns", []):
            col_name = column["name"]

            # ========== Case 1: IQR-based ==========
            if column["min_value"] == "IQR" or column["max_value"] == "IQR":
                # Collect values for this column into list
                values = fact_df.select(col(col_name)).na.drop().rdd.map(lambda r: r[0]).collect()
                
                if not values:  # skip if empty
                    continue

                iqrobj = median(values)
                q1, q2, q3 = iqrobj.calculate_iqr()  # returns [q1, median, q3] from median class

                cond = (col(col_name) < q1) | (col(col_name) > q3)
                count = fact_df.filter(cond).count()
                range_errors_count += count

                if count > 0:
                    error_subset = fact_df.filter(cond) \
                                          .withColumn("dq_error_type", lit("IQR Check").cast(StringType())) \
                                          .withColumn("dq_error_code", lit(config_business_rule["error_code"])) \
                                          .withColumn(
                                              "dq_error_message",
                                              lit(config_business_rule["error_message"].format(
                                                  table_name=config_business_rule["table_name"],
                                                  column=col_name
                                              ))
                                          )
                    error_dfs.append(error_subset)

            # ========== Case 2: Fixed range ==========
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
                                          .withColumn(
                                              "dq_error_message",
                                              lit(config_business_rule["error_message"].format(
                                                  table_name=config_business_rule["table_name"],
                                                  column=col_name
                                              ))
                                          )
                    error_dfs.append(error_subset)

    except Exception as e:
        print(f"Unrealistic quantity check failed: {e}")

    
    # ========================== 4. Write Error Records ================================
    error_df = None
    if error_dfs:
        error_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), error_dfs).dropDuplicates()
        error_tbl = f"{catalog}.{schema}.{fact_table_name.split('.')[-1]}_error"

        write_mode = "append" if spark.catalog.tableExists(error_tbl) else "overwrite"
        error_df.write.mode(write_mode).format("delta").option("mergeSchema", "true").saveAsTable(error_tbl)
        print(f"Error records {'appended to' if write_mode == 'append' else 'written to'} {error_tbl}")
    else:
        print("No error records to write for unrealistic quantity check.")

    # ============================ 5. Write Summary into dq_check ==============================
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
    summary_df.write.mode("append").format("delta").saveAsTable(f"{catalog}.{schema}.dq_check")

    # =========================== 6. Return Good Records ========================
    if error_df:
        good_df = fact_df.subtract(error_df.drop("dq_error_type", "dq_error_code", "dq_error_message"))
        return good_df
    else:
        return fact_df
