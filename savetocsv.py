# export_to_csv.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Export User Purchases to CSV") \
    .getOrCreate()

# Load the data
data_path = "C:/Users/mural/OneDrive/Desktop/SEM5/user_activity_data.txt"  
df = spark.read.json(data_path)

# Convert user_id and product_id columns to integer
df = df.withColumn("user_id", col("user_id").cast("integer")) \
       .withColumn("product_id", col("product_id").cast("integer"))

# Filter the DataFrame to include only purchase events
# Add the product_type column to the selection
purchased_df = df.filter(df.event_type == "purchase").select("user_id", "product_id", "product_name", "product_type","event_type","timestamp")

# Write the filtered data to a CSV file for Tableau
output_path = "C:/Users/mural/OneDrive/Desktop/SEM5/user_purchases1.csv"
purchased_df.write.csv(output_path, header=True, mode="overwrite")

print(f"Data successfully written to {output_path}")
