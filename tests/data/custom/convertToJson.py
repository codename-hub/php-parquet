from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# important for comparison
spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields", False)

p1 = spark.read.parquet('./supercomplex1.parquet')
p1.write.mode('overwrite').json("export_supercomplex1.spark.json")

p2 = spark.read.parquet('./complex_structs1.parquet')
p2.write.mode('overwrite').json("export_complex_structs1.spark.json")
