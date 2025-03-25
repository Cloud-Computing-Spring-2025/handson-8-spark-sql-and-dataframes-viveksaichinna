from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark Session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# ✅ Filter only verified users
verified_users_df = users_df.filter(col("Verified") == True)

# ✅ Join posts with verified users
joined_df = posts_df.join(verified_users_df, on="UserID")

# ✅ Calculate reach (Likes + Retweets) and sum for each verified user
top_verified = joined_df.withColumn("Reach", col("Likes") + col("Retweets")) \
    .groupBy("Username") \
    .agg(_sum("Reach").alias("TotalReach")) \
    .orderBy(col("TotalReach").desc()) \
    .limit(5)

# ✅ Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)