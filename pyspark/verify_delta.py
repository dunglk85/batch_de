from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("verify").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
for t in ["customers","products","transactions"]:
    df = spark.read.format("delta").load(f"/data/delta/bronze/{t}")
    print(f"{t}: {df.count()} rows, {len(df.columns)} cols")
spark.stop()
