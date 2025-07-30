from datetime import datetime, timedelta

# Generate dynamic date_max (today + 1)
today_plus_1 = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

key_column = ["shipment_sk"]

config = {
    "fact_table": f"{catalog}.silver_ods.wms_shipment",
    "primary_keys": ["shipment_sk"],  
    "essential_fields": [],
    "foreign_keys": [
        {
            "fk_column": "dim_operating_unit_sk",
            "dim_table": f"{catalog}.gold_shared_dimensions.dim_operating_unit",
            "dim_fk_column": "operating_unit_sk"
        },
        {
            "fk_column": "distribution_center_sk",
            "dim_table": f"{catalog}.silver_ods.distribution_center",
            "dim_fk_column": "distribution_center_sk"
        },
        {
            "fk_column": "dim_calendar_shipment_created_dt_sk",
            "dim_table": f"{catalog}.gold_shared_dimensions.dim_calendar",
            "dim_fk_column": "calendar_sk"
        }
    ],
    "range_column": "shipment_sk",
    "range_min": 20,
    "range_max": 500,
    "date_column": "created_dtts",
    "date_min": "2020-01-01",
    "date_max": today_plus_1,
    "enable_null_check": True,
    "enable_range_check": False,
    "enable_referential_check": True
}
