from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("verify-gold").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0").getOrCreate()
print("\n=== GOLD LAYER ===")
tables = [
    ("daily_sales_fact", "Aggregated daily sales by customer/product"),
    ("customer_metrics", "Customer lifetime value, risk scoring"),
    ("product_metrics", "Product performance KPIs"),
    ("category_trends", "Category sales trends with 7d MA"),
]
for name, desc in tables:
    df = spark.read.format("delta").load(f"/data/delta/gold/{name}")
    print(f"\n  {name} ({desc})")
    print(f"    Rows: {df.count()}, Cols: {len(df.columns)}")
    print(f"    Columns: {df.columns}")
print("\n=== MEDALLION ARCHITECTURE COMPLETE ===")
spark.stop()
