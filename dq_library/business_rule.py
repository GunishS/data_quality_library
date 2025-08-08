from datetime import datetime
from pyspark.sql.functions import col, count, isnan, lit, when
from pyspark.sql.types import NumericType,LongType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
from functools import reduce
import pyspark.sql.functions as F
from .median import median

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

class data_quality:
    def __init__(self):
        pass
    
    def unrealstic_quantity(self, table_name_1, column, config_business_rule):
        table_name = config_business_rule["table_name"]
        columns = config_business_rule["columns"]
        
        for column in columns:
            # creating the obj of median call here
            m1 =  median(col(column["name"]))
            iqr = m1.calculate_iqr()
            try:
                if (column["min_value"] == "IQR" or column["max_value"]== "IQR"):
                    q1_outlier = table_name.filter(col(column["name"]) <= iqr[0]).count()
                    q3_outlier = table_name.filter(col(column["name"]) >= iqr[2]).count()

                else:
                    range_count = table_name.filter(col(column["name"]) > col(column["max_value"]) or col(column["name"]) < col(column["min_value"])).count()
                    # q3_outlier = table_name.filter(col(column["name"]) < col(column["min_value"])).count()
                if q1_outlier:
             if (q1_outlier > 0 or q3_outlier >0 or range_count>0):
                range_errors = fact_df.filter(range_filter) \
                                      .withColumn("dq_error_type", lit("Range Check").cast(StringType()))
                error_dfs.append(range_errors)
            except Exception as e:
                print(f"Range check failed: {e}")
                range_errors_count = 0



                
        
        # return 

dq_1 = data_quality()
dq_1.unrealstic_quantity("invoice_line_item", "quantity", config_business_rule)