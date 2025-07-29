#!/usr/bin/env python3
"""
Direct prop bet analysis runner - bypasses Kafka streaming
"""
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
        
        print(f"📊 Loaded {df_stats.count()} player statistics records")
        
        # === Step 4: Calculate averages per stat type ===
        print("📈 Calculating player averages by stat type...")
        df_averages = df_stats.groupBy("personid", "stat_type") \
            .agg(avg("stat_value").alias("avg_stat_value"))
        
        print(f"📈 Calculated {df_averages.count()} player-stat averages")
        
        # === Step 5: Join prop bets with averages ===
        print("🔗 Joining prop bets with player averages...")
        df_result = df_props_with_id.join(
            df_averages,
            (df_props_with_id.personid == df_averages.personid) &
            (df_props_with_id.stat == df_averages.stat_type),
            "inner"
        ).select(
            df_props_with_id.personid,
            df_props_with_id.player_name,
            df_props_with_id.stat,
            df_props_with_id.line,
            df_averages.avg_stat_value.alias("predicted_value"),
            # Calculate deviation percentage
            (((df_averages.avg_stat_value - df_props_with_id.line) / df_props_with_id.line) * 100).alias("deviation_percentage"),
            # Determine bet outcome based on predicted vs line
            when(df_averages.avg_stat_value > df_props_with_id.line, "over")
            .when(df_averages.avg_stat_value < df_props_with_id.line, "under")
            .otherwise("push").alias("bet_outcome")
        )
        
        print(f"🔗 Joined result: {df_result.count()} records")
        df_result.show()
        
        # === Step 6: Filter for locks (deviation >= 5%) ===
        print("🔒 Filtering for locks (deviation >= 5%)...")
        df_locks_only = df_result.filter(col("deviation_percentage") >= 5.0)
        
        locks_count = df_locks_only.count()
        print(f"🔒 Found {locks_count} locks with >= 5% deviation")
        
        if locks_count == 0:
            print("⚠️ No locks found - skipping database insert")
            return
            
        df_locks_only.show()
        
        # === Step 7: Format for 'predictions' table schema ===
        print("📋 Formatting locks for database...")
        df_final = df_locks_only \
            .withColumnRenamed("stat", "stat_type") \
            .withColumn("game_date", current_date()) \
            .withColumn("team", lit(None).cast("string")) \
            .select(
                "personid", "player_name", "stat_type", "line", "game_date",
                "team", "predicted_value", "bet_outcome", "deviation_percentage"
            )
        
        print(f"📋 Final locks ready for database: {df_final.count()} records")
        df_final.show()
        
        # === Step 8: Save to database ===
        print("💾 Saving locks to database...")
        
        df_final.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
            .option("dbtable", "predictions") \
            .option("user", "sergio") \
            .option("password", "mypassword") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
            
        print(f"✅ Successfully saved {df_final.count()} locks to database!")
        
        print("✅ Prop bet prediction pipeline completed successfully!")
        print(f"📊 Processed trigger message: {trigger_message}")
        
    except Exception as e:
        print(f"❌ Error processing prop bet analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PropBetDirectAnalysis") \
        .getOrCreate()

    print("🚀 Running direct prop bet analysis...")
    process_prop_bet_analysis(spark, "direct_trigger")
    
    spark.stop()
    print("🏁 Analysis complete!")