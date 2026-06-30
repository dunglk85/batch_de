from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("verify-silver").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0").getOrCreate()
for layer in ["bronze", "silver"]:
    print(f"\n=== {layer.upper()} ===")
    for t in ["customers","products","transactions"]:
        df = spark.read.format("delta").load(f"/data/delta/{layer}/{t}")
        valid = df.filter("dq_is_valid = true").count() if layer == "silver" else "N/A"
        print(f"  {t}: {df.count()} rows, {len(df.columns)} cols, valid={valid}")
spark.stop()
