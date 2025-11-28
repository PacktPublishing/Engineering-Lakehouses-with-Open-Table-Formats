CREATE TABLE delta_uniform_table (
  id INT,
  name STRING,
  age INT,
  city STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true', 
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
LOCATION 's3://my-delta-uniform-table/';
