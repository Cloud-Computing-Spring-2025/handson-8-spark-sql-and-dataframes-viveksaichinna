from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# ✅ Split the Hashtags column, explode into individual rows, and count frequency
hashtag_counts = posts_df \
    .withColumn("Hashtag", explode(split(col("Hashtags"), ","))) \
    .groupBy("Hashtag") \
    .count() \
    .orderBy(desc("count"))

# ✅ Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)