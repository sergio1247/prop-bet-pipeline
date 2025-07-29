from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("TestKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

print("🚀 Spark session started - checking for Kafka messages...")

# Define schema for Kafka messages
kafka_schema = StructType([
    StructField("job_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Read from Kafka topic
df_kafka = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "spark-jobs") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON messages
df_messages = df_kafka.select(
    col("timestamp").alias("kafka_timestamp"),
    from_json(col("value").cast("string"), kafka_schema).alias("message")
).select("kafka_timestamp", "message.*")

print(f"📨 Found {df_messages.count()} messages in Kafka topic:")
df_messages.show(truncate=False)

# Process each message
messages = df_messages.collect()
for msg in messages:
    print(f"🔄 Processing message: job_type={msg.job_type}, timestamp={msg.timestamp}")
    
    if msg.job_type == "prop_bet_analysis":
        print("📊 Running prop bet analysis...")
        print("✅ Prop bet analysis completed!")
    else:
        print(f"⚠️  Unknown job type: {msg.job_type}")

print("✅ Kafka consumer test completed successfully!")