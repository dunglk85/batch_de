import os
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
s = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
print("Spark OK:", s.version)
s.stop()
