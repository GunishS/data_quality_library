config_business_rule = {
    "table_name":"invoice_line_item",
    "columns":[
        {"name": "quantity", "min_value": "IQR", "max_value": "IQR"},
        {"name": "order", "min_value": 1, "max_value": 100}
    ],
    "error_message":"Unrealistic quantity found in {table_name} for {column}",
    "error_code":"DQ001",
}