from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("check-schema").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0").getOrCreate()
for t in ["customers","products","transactions"]:
    df = spark.read.format("delta").load(f"/data/delta/silver/{t}")
    print(f"\n=== SILVER {t} ===")
    print(f"Count: {df.count()}")
    print(f"Columns: {df.columns}")
    print(f"Schema: {df.schema.simpleString()}")
spark.stop()
