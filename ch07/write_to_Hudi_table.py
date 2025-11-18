from pyspark.sql.types import *
table_name = "people"
local_base_path = "file:/tmp/hudi-dataset"
records = [
   (1, 'John', 25, 'NYC', '2023-09-28 00:00:00'),
   (2, 'Emily', 30, 'SFO', '2023-09-28 00:00:00'),
   (3, 'Michael', 35, 'ORD', '2023-09-28 00:00:00'),
   (4, 'Andrew', 40, 'NYC', '2023-10-28 00:00:00'),
   (5, 'Bob', 28, 'SEA', '2023-09-23 00:00:00'),
   (6, 'Charlie', 31, 'DFW', '2023-08-29 00:00:00')
]
schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True),
   StructField("city", StringType(), True),
   StructField("create_ts", StringType(), True)
])
df = spark.createDataFrame(records, schema)
hudi_options = {
   'hoodie.table.name': table_name,
   'hoodie.datasource.write.partitionpath.field': 'city',
   'hoodie.datasource.write.hive_style_partitioning': 'true'
}
(
   df.write
   .format("hudi")
   .options(**hudi_options)
   .save(f"{local_base_path}/{table_name}")
)