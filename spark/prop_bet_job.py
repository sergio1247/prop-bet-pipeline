from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, when, lit, split, trim, current_date
from pyspark.sql.types import StructType, StructField, StringType
import json

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("PropBetKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

print("🚀 Spark session started - checking for Kafka messages...")

# Define schema for Kafka messages
kafka_schema = StructType([
    StructField("job_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])

try:
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
            print("📊 Running comprehensive prop bet analysis...")
            
            # === Step 1: Load prop bets JSON ===
            print("📂 Loading prop bets data...")
            df_props = spark.read.json("/app/data/prop_bets.json")
            
            # Split player_name into firstname and lastname
            df_props = df_props.withColumn("firstname", trim(split("player_name", " ")[0])) \
                               .withColumn("lastname", trim(split("player_name", " ")[1]))
            
            print(f"🎯 Loaded {df_props.count()} prop bets")
            df_props.show(5)
            
            # === Step 2: Load Players table from Postgres ===
            print("👥 Loading players data from Postgres...")
            df_players = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
                .option("dbtable", "Players") \
                .option("user", "sergio") \
                .option("password", "mypassword") \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .dropDuplicates(["personid"])
            
            print(f"👥 Loaded {df_players.count()} players")
            
            # Join props with Players to get personid
            print("🔗 Joining prop bets with player data...")
            df_props_with_id = df_props.join(
                df_players,
                (df_props.firstname == df_players.firstname) &
                (df_props.lastname == df_players.lastname),
                "inner"
            ).select("player_name", "stat", "line", "personid")
            
            print(f"🔗 Matched {df_props_with_id.count()} prop bets with players")
            df_props_with_id.show()
            
            # === Step 3: Load PlayerStatistics ===
            print("📊 Loading player statistics...")
            df_stats = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
                .option("dbtable", "PlayerStatistics") \
                .option("user", "sergio") \
                .option("password", "mypassword") \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print(f"📊 Loaded {df_stats.count()} statistical records")
            
            # === Step 4: Join stats with prop players only ===
            print("🔄 Filtering statistics for prop bet players...")
            df_filtered_stats = df_stats.join(df_props_with_id, on="personid", how="inner")
            
            print(f"🔄 Found {df_filtered_stats.count()} relevant statistical records")
            
            # === Step 5: Compute averages (static for 'points' stat_type) ===
            print("🧮 Computing player averages...")
            df_avg = df_filtered_stats.groupBy("personid", "player_name", "stat", "line").agg(
                avg("points").alias("predicted_value")
            )
            
            print(f"🧮 Computed averages for {df_avg.count()} player-stat combinations")
            df_avg.show()
            
            # === Step 6: Create recommendation ===
            print("🎯 Generating betting recommendations...")
            df_result = df_avg.withColumn(
                "bet_outcome",
                when(col("predicted_value") > col("line"), lit("over")).otherwise(lit("under"))
            )
            
            df_result.show()
            
            # === Step 7: Format for 'predictions' table schema ===
            print("📋 Formatting results for database...")
            df_final = df_result \
                .withColumnRenamed("stat", "stat_type") \
                .withColumn("game_date", current_date()) \
                .withColumn("team", lit(None).cast("string")) \
                .select(
                    "personid", "stat_type", "line", "game_date",
                    "team", "predicted_value", "bet_outcome"
                )
            
            print(f"📋 Final predictions ready: {df_final.count()} records")
            df_final.show()
            
            # === Step 8: Write to Postgres ===
            print("💾 Saving predictions to database...")
            df_final.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
                .option("dbtable", "predictions") \
                .option("user", "sergio") \
                .option("password", "mypassword") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            print("✅ Prop bet prediction pipeline completed successfully!")
            print(f"📊 Processed message from {msg.timestamp}")
        else:
            print(f"⚠️  Unknown job type: {msg.job_type}")
            

except Exception as e:
    print(f"❌ Error in main pipeline: {e}")
    import traceback
    traceback.print_exc()
    print("🔄 Falling back to simple database test...")
    
    # Fallback to simple database test
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
            .option("dbtable", "players") \
            .option("user", "sergio") \
            .option("password", "mypassword") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        df.show(10)
        print("✅ Successfully connected to Postgres and loaded data.")
    except Exception as fallback_e:
        print(f"❌ Fallback also failed: {fallback_e}")
        traceback.print_exc()





# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, when, col, lit, split, trim, current_date

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("PropBetPredictionPipeline") \
#     .config("spark.jars", "/app/jdbc/postgresql-42.7.3.jar") \
#     .getOrCreate()

# # === Step 1: Load prop bets JSON ===
# df_props = spark.read.json("/app/data/prop_bets.json")

# # Split player_name into firstname and lastname
# df_props = df_props.withColumn("firstname", trim(split("player_name", " ")[0])) \
#                    .withColumn("lastname", trim(split("player_name", " ")[1]))

# # === Step 2: Load Players table from Postgres ===
# df_players = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
#     .option("dbtable", "Players") \
#     .option("user", "sergio") \
#     .option("password", "mypassword") \
#     .option("driver", "org.postgresql.Driver") \
#     .load() \
#     .dropDuplicates(["id"])

# # Join props with Players to get personid
# df_props_with_id = df_props.join(
#     df_players,
#     (df_props.firstname == df_players.firstname) &
#     (df_props.lastname == df_players.lastname),
#     "inner"
# ).select("player_name", "stat", "line", "id") \
#  .withColumnRenamed("id", "personid")

# # === Step 3: Load PlayerStatistics ===
# df_stats = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
#     .option("dbtable", "PlayerStatistics") \
#     .option("user", "sergio") \
#     .option("password", "mypassword") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

# # === Step 4: Join stats with prop players only ===
# df_filtered_stats = df_stats.join(df_props_with_id, on="personid", how="inner")

# # === Step 5: Compute averages (static for 'points' stat_type) ===
# df_avg = df_filtered_stats.groupBy("personid", "player_name", "stat", "line").agg(
#     avg("points").alias("predicted_value")
# )

# # === Step 6: Create recommendation ===
# df_result = df_avg.withColumn(
#     "bet_outcome",
#     when(col("predicted_value") > col("line"), lit("over")).otherwise(lit("under"))
# )

# # === Step 7: Format for 'predictions' table schema ===
# df_final = df_result \
#     .withColumnRenamed("stat", "stat_type") \
#     .withColumn("game_date", current_date()) \
#     .withColumn("team", lit(None).cast("string")) \
#     .select(
#         "personid", "stat_type", "line", "game_date",
#         "team", "predicted_value", "bet_outcome"
#     )

# # === Step 8: Write to Postgres ===
# df_final.write \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
#     .option("dbtable", "predictions") \
#     .option("user", "sergio") \
#     .option("password", "mypassword") \
#     .option("driver", "org.postgresql.Driver") \
#     .mode("append") \
#     .save()

# print("✅ Prediction pipeline completed successfully.")
