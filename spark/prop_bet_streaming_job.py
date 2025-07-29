from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, when, lit, split, trim, current_date, current_timestamp, date_sub
from pyspark.sql.types import StructType, StructField, StringType
import json

def process_prop_bet_analysis(spark, trigger_message):
    """Process a single prop bet analysis trigger"""
    try:
        print(f"🔄 Processing trigger: {trigger_message}")
        
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
        
        # === Step 6: Create recommendation and calculate deviation ===
        print("🎯 Generating betting recommendations...")
        df_result = df_avg.withColumn(
            "bet_outcome",
            when(col("predicted_value") > col("line"), lit("over")).otherwise(lit("under"))
        ).withColumn(
            "deviation_percentage",
            when(col("predicted_value") > col("line"),
                ((col("predicted_value") - col("line")) / col("line")) * 100
            ).otherwise(
                ((col("line") - col("predicted_value")) / col("line")) * 100
            )
        )
        
        print("📊 All predictions with deviations:")
        df_result.show()
        
        # === Step 7: Filter for LOCKS ONLY (>5% deviation) ===
        print("🔒 Filtering for locks (>5% deviation)...")
        df_locks_only = df_result.filter(col("deviation_percentage") >= 5.0)
        
        locks_count = df_locks_only.count()
        print(f"🔒 Found {locks_count} locks out of {df_result.count()} predictions")
        
        if locks_count == 0:
            print("⚠️ No locks found - skipping database insert")
            return
            
        df_locks_only.show()
        
        # === Step 8: Format for 'predictions' table schema ===
        print("📋 Formatting locks for database...")
        df_final = df_locks_only \
            .withColumnRenamed("stat", "stat_type") \
            .withColumn("game_date", current_date()) \
            .withColumn("team", lit(None).cast("string")) \
            .withColumn("created_at", current_timestamp()) \
            .select(
                "personid", "player_name", "stat_type", "line", "game_date",
                "team", "predicted_value", "bet_outcome", "deviation_percentage", "created_at"
            )
        
        print(f"📋 Final locks ready for database: {df_final.count()} records")
        df_final.show()
        
        # === Step 9: Check for existing predictions and write locks to database ===
        print("💾 Checking for existing predictions and saving locks...")
        
        try:
            # Read existing predictions to check for exact duplicates
            df_existing = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
                .option("dbtable", "predictions") \
                .option("user", "sergio") \
                .option("password", "mypassword") \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            existing_count = df_existing.count()
            print(f"📋 Found {existing_count} total existing predictions")
            
            if existing_count > 0:
                # Create a composite key for exact duplicate detection
                df_existing_keys = df_existing.select("personid", "stat_type", "line", "game_date").distinct()
                
                # Anti-join to exclude exact duplicates (same personid, stat_type, line, game_date)
                df_new_only = df_final.join(
                    df_existing_keys,
                    on=["personid", "stat_type", "line", "game_date"],
                    how="left_anti"
                )
                
                new_count = df_new_only.count()
                print(f"🆕 Found {new_count} truly new predictions (not duplicates)")
                
                if new_count > 0:
                    # Additional deduplication within current batch
                    df_dedup = df_new_only.dropDuplicates(["personid", "stat_type", "line", "game_date"])
                    
                    df_dedup.write \
                        .format("jdbc") \
                        .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
                        .option("dbtable", "predictions") \
                        .option("user", "sergio") \
                        .option("password", "mypassword") \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("append") \
                        .save()
                        
                    print(f"✅ Successfully saved {df_dedup.count()} new predictions to database!")
                else:
                    print("⚠️ No new predictions to save (all are duplicates of existing data)")
            else:
                # No existing predictions, insert all with deduplication
                print("📋 No existing predictions, inserting all")
                
                # Deduplicate within current batch
                df_dedup = df_final.dropDuplicates(["personid", "stat_type", "line", "game_date"])
                
                df_dedup.write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
                    .option("dbtable", "predictions") \
                    .option("user", "sergio") \
                    .option("password", "mypassword") \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                    
                print(f"✅ Successfully saved {df_dedup.count()} predictions to database!")
                
        except Exception as e:
            print(f"Error checking for duplicates: {e}")
            print("Proceeding with safe insert...")
            
            # Fallback: safe insert with strong deduplication
            df_dedup = df_final.dropDuplicates(["personid", "stat_type", "line", "game_date"])
            
            df_dedup.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
                .option("dbtable", "predictions") \
                .option("user", "sergio") \
                .option("password", "mypassword") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
                
            print(f"✅ Successfully saved {df_dedup.count()} predictions to database!")
        
        print("✅ Prop bet prediction pipeline completed successfully!")
        print(f"📊 Processed trigger message: {trigger_message}")
        
    except Exception as e:
        print(f"❌ Error processing prop bet analysis: {e}")
        import traceback
        traceback.print_exc()

def main():
    # Initialize Spark session with Kafka support
    spark = SparkSession.builder \
        .appName("PropBetStreamingConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .getOrCreate()

    print("🚀 Spark Streaming session started - listening for Kafka messages...")

    # Define schema for Kafka messages
    kafka_schema = StructType([
        StructField("job_type", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    try:
        # Read streaming data from Kafka topic
        df_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "spark-jobs") \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON messages from Kafka
        df_parsed = df_stream.select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), kafka_schema).alias("message")
        ).select("kafka_timestamp", "message.*")

        print("📡 Kafka stream initialized, waiting for messages...")

        # Process each batch of messages
        def process_batch(batch_df, batch_id):
            print(f"📦 Processing batch {batch_id}")
            
            if batch_df.count() > 0:
                print(f"📨 Received {batch_df.count()} messages in batch {batch_id}")
                batch_df.show()
                
                # Process each message in the batch
                messages = batch_df.collect()
                for row in messages:
                    if row.job_type == "prop_bet_analysis":
                        print(f"🎯 Triggering prop bet analysis for message: {row.job_type} at {row.timestamp}")
                        process_prop_bet_analysis(spark, f"{row.job_type} - {row.timestamp}")
                    else:
                        print(f"⚠️ Unknown job type: {row.job_type}")
            else:
                print(f"📦 Batch {batch_id}: No new messages")

        # Start the streaming query
        query = df_parsed.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/kafka-checkpoint") \
            .trigger(processingTime='30 seconds') \
            .start()

        print("🎧 Streaming job started! Listening for Kafka messages...")
        print("📢 Send messages to 'spark-jobs' topic to trigger prop bet analysis")
        print("🛑 Press Ctrl+C to stop the streaming job")
        
        # Wait for the streaming to finish
        query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Streaming error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()