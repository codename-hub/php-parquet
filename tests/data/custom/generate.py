from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# NOTE: you may have to clear the directories, if present:
# supercomplex1.spark.parquet
# complex_structs1.spark.parquet

p1 = spark.read.parquet('./supercomplex1.parquet')
p1.coalesce(1).write.format("parquet").parquet('./tmp-supercomplex1.spark.parquet')
# this will output a subdir with the above name
# we have to copy over the *.parquet file(s) manually

p2 = spark.read.parquet('./complex_structs1.parquet')
p2.coalesce(1).write.format("parquet").parquet('./tmp-complex_structs1.spark.parquet')
# this will output a subdir with the above name
# we have to copy over the *.parquet file(s) manually
