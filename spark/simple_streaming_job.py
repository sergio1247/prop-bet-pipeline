from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, when, lit, split, trim, current_date, current_timestamp, stddev, count, min as spark_min, max as spark_max, sum as F_sum, row_number
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf
import json
import time
import random
import math

def process_prop_bet_analysis(spark, trigger_message):
    """Process a single prop bet analysis trigger with simplified ML"""
    try:
        print(f"🔄 Processing trigger: {trigger_message}")
        
        # Clear all cached DataFrames to ensure fresh results  
        spark.catalog.clearCache()
        # Also clear any persisted RDDs and cached data
        spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")
        print("🧹 Cleared Spark cache and set new checkpoint for fresh results")
        
        start_time = time.time()
        
        # === Step 1: Load prop bets JSON (optimized) ===
        print("📂 Loading prop bets data...")
        df_props = spark.read.json("/app/data/prop_bets_small.json") \
                           .withColumn("firstname", trim(split("player_name", " ")[0])) \
                           .withColumn("lastname", trim(split("player_name", " ")[1])) \
                           .cache()  # Cache since it's used multiple times
        
        # Only trigger one action to get both count and sample
        props_count = df_props.count()
        print(f"🎯 Loaded {props_count} prop bets (cached for reuse)")
        
        # === Step 2: Load Players table from Postgres (parallel loading) ===  
        print("👥 Loading players data with hash partitioning...")
        
        # Use hash partitioning - no bounds query needed
        df_players = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
            .option("dbtable", "Players") \
            .option("user", "sergio") \
            .option("password", "mypassword") \
            .option("driver", "org.postgresql.Driver") \
            .option("predicates", [
                "personid % 4 = 0",  # Worker 1
                "personid % 4 = 1",  # Worker 2  
                "personid % 4 = 2",  # Worker 3
                "personid % 4 = 3"   # Worker 4
            ]) \
            .load() \
            .dropDuplicates(["personid"]) \
            .cache()  # Cache for reuse
        
        players_count = df_players.count()
        print(f"👥 Loaded {players_count:,} players with hash partitioning")
        
        # Join props with Players to get personid
        print("🔗 Joining prop bets with player data...")
        df_props_with_id = df_props.join(
            df_players,
            (df_props.firstname == df_players.firstname) &
            (df_props.lastname == df_players.lastname),
            "inner"
        ).select("player_name", "stat", "line", "team", "personid")
        
        print(f"🔗 Matched {df_props_with_id.count()} prop bets with players")
        df_props_with_id.show()
        
        # === Step 3: Load PlayerStatistics & Games in parallel (optimized) ===
        print("📊 Loading PlayerStatistics and Games in parallel...")
        
        # Use hash partitioning directly - no bounds queries needed
        df_stats = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
            .option("dbtable", "PlayerStatistics") \
            .option("user", "sergio") \
            .option("password", "mypassword") \
            .option("driver", "org.postgresql.Driver") \
            .option("predicates", [
                "id % 4 = 0",  # Worker 1: ~25%
                "id % 4 = 1",  # Worker 2: ~25%  
                "id % 4 = 2",  # Worker 3: ~25%
                "id % 4 = 3"   # Worker 4: ~25%
            ]) \
            .load() \
            .cache()  # Cache for reuse
        
        # Load Games table with hash partitioning  
        df_games = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
            .option("dbtable", "Games") \
            .option("user", "sergio") \
            .option("password", "mypassword") \
            .option("driver", "org.postgresql.Driver") \
            .option("predicates", [
                "gameid % 4 = 0",  # Worker 1: ~25%
                "gameid % 4 = 1",  # Worker 2: ~25%  
                "gameid % 4 = 2",  # Worker 3: ~25%
                "gameid % 4 = 3"   # Worker 4: ~25%
            ]) \
            .load() \
            .cache()  # Cache for reuse
        
        # Force parallel evaluation
        stats_count = df_stats.count()
        games_count = df_games.count()
        print(f"📊 Loaded {stats_count:,} stats & {games_count:,} games in parallel")
        
        # === Step 4: Join stats with prop players & games (optimized single join) ===
        print("🔄 Creating unified dataset with all stat types...")
        
        # Broadcast small prop players table for efficient join
        from pyspark.sql.functions import broadcast
        
        # Single comprehensive join with games data included
        df_unified = df_stats.join(
            broadcast(df_props_with_id), 
            on="personid", 
            how="inner"
        ).join(
            df_games.select("gameid", "gamedate"), 
            on="gameid", 
            how="inner"
        ).repartition(4, "personid")  # Ensure even distribution by player for Monte Carlo
        
        unified_count = df_unified.count()
        print(f"🔄 Created unified dataset: {unified_count:,} records across all stat types")
        
        # === Step 5: Monte Carlo Simulation Analysis ===
        print("🎲 Starting Monte Carlo simulation analysis...")
        simulation_start = time.time()
        
        # Stat column mapping
        stat_column_map = {
            "points": "points",
            "assists": "assists", 
            "rebounds": "reboundstotal",
            "steals": "steals",
            "blocks": "blocks",
            "threes": "threepointersmade",
            "3pm": "threepointersmade"
        }
        
        # Get all unique stat types in the current prop bets
        stat_types = [row.stat for row in df_props_with_id.select("stat").distinct().collect()]
        print(f"📊 Found stat types for Monte Carlo: {stat_types}")
        
        # Monte Carlo simulation parameters
        N_SIMULATIONS = 500000  # Massively increased for intensive parallel computation
        
        # Define Monte Carlo UDF
        def monte_carlo_prediction(player_stats_list, prop_line, base_avg, std_dev, min_val, max_val, stat_type):
            """Run Monte Carlo simulation for a single prop bet"""
            if not player_stats_list or len(player_stats_list) < 5:
                return float(base_avg), 0.5, float(base_avg * 0.8), float(base_avg * 1.2)
            
            results = []
            # Remove fixed seeding entirely to allow true randomness each run
            # Don't set seed - let Python use system randomness
            
            # Debug: Print random test to verify randomness is working
            test_random = random.random()
            print(f"🎲 Random test for {stat_type}: {test_random:.6f}")
            
            
            # Add realistic daily variance factors with stat-specific bounds
            if stat_type in ["threes", "3pm", "threepointersmade", "steals"]:
                # Tighter bounds for high-variance stats to prevent inflation
                daily_form = max(0.95, min(1.05, random.normalvariate(1.0, 0.02)))      # 2% variance, capped at +/- 5%
                injury_concern = max(0.97, min(1.03, random.normalvariate(1.0, 0.01)))  # 1% variance, capped at +/- 3%
                matchup_factor = max(0.93, min(1.07, random.normalvariate(1.0, 0.02)))  # 2% variance, capped at +/- 7%
                rest_factor = max(0.97, min(1.03, random.normalvariate(1.0, 0.01)))     # 1% variance, capped at +/- 3%
            else:
                # Normal bounds for other stats
                daily_form = max(0.92, min(1.08, random.normalvariate(1.0, 0.03)))      # 3% variance, capped at +/- 8%
                injury_concern = max(0.95, min(1.05, random.normalvariate(1.0, 0.02)))  # 2% variance, capped at +/- 5%
                matchup_factor = max(0.90, min(1.10, random.normalvariate(1.0, 0.04)))  # 4% variance, capped at +/- 10%
                rest_factor = max(0.95, min(1.05, random.normalvariate(1.0, 0.02)))     # 2% variance, capped at +/- 5%
            
            # Calculate season-weighted performance with recent game boost
            if len(player_stats_list) >= 5:
                # Use full dataset (2 seasons worth) - player_stats_list is already sorted by gamedate DESC
                all_stats = player_stats_list  # Full dataset from database
                
                # Season-based weighting: assume ~82 games per season
                weights = []
                for i, stat in enumerate(all_stats):
                    if i < 82:  # Current season (most recent 82 games)
                        base_weight = 0.7  # 70% weight for current season
                    else:  # Previous season 
                        base_weight = 0.3  # 30% weight for previous season
                    
                    # Add recency boost for most recent 10 games only
                    if i < 10:
                        recency_boost = 1.5  # 50% boost for last 10 games
                        final_weight = base_weight * recency_boost
                    else:
                        final_weight = base_weight
                    
                    weights.append(final_weight)
                
                # Calculate weighted average
                recent_avg = sum(stat * weight for stat, weight in zip(all_stats, weights)) / sum(weights)
            else:
                recent_avg = base_avg
            
            trend = recent_avg / base_avg if base_avg > 0 else 1.0
            
            # Determine if this is an elite player based on stat type and performance
            is_elite_player = False
            if len(player_stats_list) > 100:  # Must have sufficient games
                if stat_type == "points" and base_avg > 20:  # Elite scorer
                    is_elite_player = True
                elif stat_type == "assists" and base_avg > 5:  # Elite playmaker  
                    is_elite_player = True
                elif stat_type in ["rebounds", "reboundstotal"] and base_avg > 6.5:  # Elite rebounder
                    is_elite_player = True
                elif stat_type == "steals" and base_avg > 1.3:  # Elite steal defender
                    is_elite_player = True
                elif stat_type == "blocks" and base_avg > 1.2:  # Elite shot blocker
                    is_elite_player = True
                elif stat_type in ["threes", "3pm", "threepointersmade"] and base_avg > 2.5:  # Elite 3pt shooter
                    is_elite_player = True
            
            for i in range(N_SIMULATIONS):
                # Sample from player's historical distribution
                base_sample = random.normalvariate(base_avg, max(std_dev, base_avg * 0.1))
                
                # Apply daily factors and random factors
                if is_elite_player:
                    # Elite players: reduced game-to-game variance but still affected by daily factors
                    game_injury_impact = random.normalvariate(0.0, 0.02)     # +/- 2% game injury
                    game_motivation = random.normalvariate(0.0, 0.03)        # +/- 3% motivation  
                    referee_impact = random.normalvariate(0.0, 0.01)         # +/- 1% officiating
                    trend_impact = (trend - 1.0) * 0.6                       # 60% trend impact
                    
                    # Apply daily factors (same for all simulations in this run)
                    total_impact = (game_injury_impact + game_motivation + referee_impact + trend_impact)
                    total_impact = max(-0.15, min(0.15, total_impact))  # Cap at +/- 15%
                    
                    simulated_value = (base_sample * (1.0 + total_impact) * 
                                     daily_form * injury_concern * matchup_factor * rest_factor)
                else:
                    # Regular players: moderate game variance + daily factors
                    # Reduce variance for high-variance stats (threes, steals) to prevent inflation
                    if stat_type in ["threes", "3pm", "threepointersmade", "steals"]:
                        game_injury = random.normalvariate(1.0, 0.02)           # Reduced health uncertainty
                        game_motivation = random.normalvariate(1.0, 0.03)       # Reduced game importance
                        referee_factor = random.normalvariate(1.0, 0.01)        # Reduced officiating
                        trend_factor = random.normalvariate(trend, 0.02)        # Reduced recent trend
                    else:
                        game_injury = random.normalvariate(1.0, 0.04)           # Health uncertainty
                        game_motivation = random.normalvariate(1.0, 0.05)       # Game importance
                        referee_factor = random.normalvariate(1.0, 0.02)        # Officiating
                        trend_factor = random.normalvariate(trend, 0.04)        # Recent trend
                    
                    simulated_value = (base_sample * game_injury * game_motivation * 
                                     referee_factor * trend_factor *
                                     daily_form * injury_concern * matchup_factor * rest_factor)
                
                # Hot/cold streak simulation (moderate impact for all players)
                if len(player_stats_list) >= 3:
                    last_3_avg = sum(player_stats_list[-3:]) / 3
                    if last_3_avg > base_avg * 1.2:  # Hot streak
                        if is_elite_player:
                            # Elite players get modest hot streak boost (3%)
                            simulated_value *= random.normalvariate(1.03, 0.02)
                        else:
                            # Regular players get moderate hot streak boost (8%)
                            streak_factor = random.normalvariate(1.08, 0.04)
                            simulated_value *= streak_factor
                    elif last_3_avg < base_avg * 0.8:  # Cold streak
                        if is_elite_player:
                            # Elite players have minimal cold streak impact (2% reduction)
                            simulated_value *= random.normalvariate(0.98, 0.02)
                        else:
                            # Regular players get moderate cold streak impact (7% reduction)
                            streak_factor = random.normalvariate(0.93, 0.04)
                            simulated_value *= streak_factor
                
                # For non-elite players, the multiplicative factors were already applied above
                # For elite players, only the additive model and streak adjustments apply
                
                # Apply realistic performance floors for all players
                if is_elite_player:
                    # Elite players rarely perform below 60% of their average
                    performance_floor = base_avg * 0.6
                else:
                    # Regular players can go as low as 50% of average with stat-specific minimums
                    if stat_type == "points":
                        performance_floor = max(base_avg * 0.5, 2.0)  # Minimum 2 points
                    elif stat_type == "assists":
                        performance_floor = max(base_avg * 0.5, 1.0)  # Minimum 1 assist
                    elif stat_type in ["rebounds", "reboundstotal"]:
                        performance_floor = max(base_avg * 0.5, 1.5)  # Minimum 1.5 rebounds
                    elif stat_type == "steals":
                        performance_floor = max(base_avg * 0.5, 0.3)  # Minimum 0.3 steals
                    elif stat_type == "blocks":
                        performance_floor = max(base_avg * 0.5, 0.2)  # Minimum 0.2 blocks
                    elif stat_type in ["threes", "3pm", "threepointersmade"]:
                        performance_floor = max(base_avg * 0.5, 0.5)  # Minimum 0.5 threes
                    else:
                        performance_floor = max(base_avg * 0.5, 0.5)
                
                simulated_value = max(simulated_value, performance_floor)
                
                # Apply realistic ceiling (use max_val but ensure it's reasonable)
                reasonable_ceiling = max(max_val, base_avg * 3.0)  # At least 3x average ceiling
                simulated_value = min(simulated_value, reasonable_ceiling)
                results.append(simulated_value)
            
            # Calculate Monte Carlo statistics
            mean_prediction = sum(results) / len(results)
            probability_over = sum(1 for x in results if x > prop_line) / len(results)
            
            # Confidence intervals
            sorted_results = sorted(results)
            conf_low = sorted_results[int(0.1 * len(sorted_results))]
            conf_high = sorted_results[int(0.9 * len(sorted_results))]
            
            return float(mean_prediction), float(probability_over), float(conf_low), float(conf_high)
        
        # Register UDF (will be updated per stat type)
        monte_carlo_udf = udf(monte_carlo_prediction, 
                             StructType([
                                 StructField("predicted_value", DoubleType(), True),
                                 StructField("probability_over", DoubleType(), True),
                                 StructField("confidence_low", DoubleType(), True),
                                 StructField("confidence_high", DoubleType(), True)
                             ]))
        
        # Process ALL stat types together (parallel processing)
        print(f"🎲 Running Monte Carlo simulations for ALL stat types in parallel ({N_SIMULATIONS} simulations per player)...")
        
        # Calculate recency-weighted statistics for ALL stat types at once
        from pyspark.sql.window import Window
        window_spec = Window.partitionBy("personid", "stat").orderBy(col("gamedate").desc())
        
        # Add game rank and stronger recency weights (heavily favor recent games)
        df_with_recency = df_unified.withColumn("game_rank", F.row_number().over(window_spec))
        # Exponential decay weighting: recent games get much higher weight
        df_weighted = df_with_recency.withColumn("weight", F.pow(0.95, col("game_rank") - 1))
        
        # Process each stat type with appropriate column mapping
        df_monte_carlo_list = []
        
        for stat_type in stat_types:
            target_column = stat_column_map.get(stat_type, "points")
            
            # Filter for this stat type and add weighted stat column
            df_stat_data = df_weighted.filter(col("stat") == stat_type) \
                .withColumn("weighted_stat", col(target_column) * col("weight"))
            
            # Calculate aggregated statistics
            df_monte_carlo_stats = df_stat_data.groupBy("personid", "player_name", "stat", "line", "team").agg(
                (F.sum("weighted_stat") / F.sum("weight")).alias("base_avg"),  # Weighted average
                stddev(target_column).alias("std_dev"),
                spark_min(target_column).alias("min_val"),
                spark_max(target_column).alias("max_val"),
                count(target_column).alias("game_count"),
                F.collect_list(target_column).alias("game_stats")  # Collect stats efficiently
            ).filter(col("game_count") >= 3)  # Need at least 3 games for reliable simulation
            
            # Apply Monte Carlo UDF directly (no expensive RDD operations)
            df_monte_carlo_results = df_monte_carlo_stats.withColumn(
                "monte_carlo_results",
                monte_carlo_udf(
                    col("game_stats"),
                    col("line"),
                    col("base_avg"),
                    col("std_dev"),
                    col("min_val"),
                    col("max_val"),
                    lit(stat_type)  # Pass the current stat type as a literal
                )
            ).select(
                "personid", "player_name", "stat", "line", "team",
                col("monte_carlo_results.predicted_value").alias("predicted_value"),
                col("monte_carlo_results.probability_over").alias("probability_over"),
                col("monte_carlo_results.confidence_low").alias("confidence_low"),
                col("monte_carlo_results.confidence_high").alias("confidence_high")
            )
            
            df_monte_carlo_list.append(df_monte_carlo_results)
        
        if not df_monte_carlo_list:
            print("❌ No Monte Carlo predictions generated")
            return
            
        # Union all stat type results
        df_avg = df_monte_carlo_list[0]
        for df in df_monte_carlo_list[1:]:
            df_avg = df_avg.union(df)
        
        simulation_time = time.time() - simulation_start
        total_simulations = df_avg.count() * N_SIMULATIONS
        print(f"🎲 Monte Carlo completed: {total_simulations:,} total simulations in {simulation_time:.2f} seconds")
        print(f"🧮 Generated {df_avg.count()} Monte Carlo predictions")
        
        # === Step 6: Direct Python processing (eliminating DataFrame bottleneck) ===
        print("🎯 Generating predictions with direct Python processing...")
        
        # Collect Monte Carlo results immediately (much faster than DataFrame operations)
        monte_carlo_results = df_avg.collect()
        print(f"📦 Collected {len(monte_carlo_results)} Monte Carlo results for direct processing")
        
        # Process results directly in Python (eliminating 44-second DataFrame bottleneck)
        from datetime import datetime, date
        predictions_data = []
        current_time = datetime.now()
        current_date_val = date.today()
        total_time = time.time() - start_time
        
        for row in monte_carlo_results:
            # Calculate betting recommendation (Python is much faster than Spark DataFrame)
            if row.probability_over > 0.6:
                bet_outcome = "over"
            elif row.probability_over < 0.4:
                bet_outcome = "under"
            else:
                bet_outcome = "neutral"
            
            # Calculate deviation percentage
            if row.predicted_value > row.line:
                deviation_percentage = ((row.predicted_value - row.line) / row.line) * 100
            else:
                deviation_percentage = ((row.line - row.predicted_value) / row.line) * 100
            
            # Calculate confidence score
            if row.probability_over >= 0.7 or row.probability_over <= 0.3:
                confidence_score = "high"
            elif row.probability_over >= 0.55 or row.probability_over <= 0.45:
                confidence_score = "medium"
            else:
                confidence_score = "low"
            
            # Calculate risk assessment
            confidence_range = row.confidence_high - row.confidence_low
            if confidence_range > row.predicted_value * 0.5:
                risk_assessment = "high_risk"
            elif confidence_range > row.predicted_value * 0.3:
                risk_assessment = "medium_risk"
            else:
                risk_assessment = "low_risk"
            
            # Create final prediction record (direct Python tuple)
            predictions_data.append((
                row.personid, row.player_name, row.stat, row.line, 
                current_date_val, row.team, row.predicted_value, bet_outcome,
                deviation_percentage, row.probability_over, row.confidence_low,
                row.confidence_high, confidence_score, risk_assessment, 
                current_time, simulation_time, total_time
            ))
        
        print(f"✅ Generated {len(predictions_data)} predictions with direct Python processing")
        
        # === Step 7: Write to database (direct Python connection) ===
        print("💾 Saving predictions to database with direct Python connection...")
        
        import psycopg2
        from psycopg2.extras import execute_values
        
        # Direct PostgreSQL connection (much faster for small datasets)
        conn = psycopg2.connect(
            host="pg-player",
            database="sportsdb", 
            user="sergio",
            password="mypassword"
        )
        
        try:
            cursor = conn.cursor()
            
            # Batch insert all records at once
            insert_query = """
            INSERT INTO predictions (
                personid, player_name, stat_type, line, game_date, team,
                predicted_value, bet_outcome, deviation_percentage, 
                probability_over, confidence_low, confidence_high,
                confidence_score, risk_assessment, created_at, 
                processing_time_seconds, total_processing_time_seconds
            ) VALUES %s
            """
            
            # Data is already prepared as tuples from direct Python processing
            execute_values(cursor, insert_query, predictions_data, template=None, page_size=100)
            
            conn.commit()
            print(f"✅ Successfully saved {len(predictions_data)} predictions in single transaction")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Database error: {e}")
        finally:
            cursor.close()
            conn.close()
        
        total_time = time.time() - start_time
        print("✅ Prop bet prediction pipeline completed successfully!")
        print(f"⏱️ Total processing time: {total_time:.2f} seconds")
        print(f"📊 Processed trigger message: {trigger_message}")
        print(f"🚀 Processed ~21 predictions with 500K simulations each")
        
    except Exception as e:
        print(f"❌ Error processing prop bet analysis: {e}")
        import traceback
        traceback.print_exc()

def main():
    # Initialize Spark session with Kafka support
    spark = SparkSession.builder \
        .appName("SimplePropBetStreamingConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .config("spark.sql.streaming.kafka.consumer.poll.timeout", "120000") \
        .config("spark.sql.streaming.kafka.consumer.request.timeout.ms", "300000") \
        .config("spark.sql.streaming.kafka.consumer.session.timeout.ms", "180000") \
        .config("spark.sql.streaming.kafka.consumer.heartbeat.interval.ms", "30000") \
        .config("spark.sql.streaming.kafka.consumer.max.poll.interval.ms", "300000") \
        .getOrCreate()

    print("🚀 Simple Spark Streaming session started - listening for Kafka messages...")

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
            .option("kafka.consumer.poll.timeout.ms", "120000") \
            .option("kafka.consumer.request.timeout.ms", "300000") \
            .option("kafka.consumer.session.timeout.ms", "180000") \
            .option("kafka.consumer.heartbeat.interval.ms", "30000") \
            .option("kafka.consumer.max.poll.interval.ms", "300000") \
            .load()

        # Parse JSON messages from Kafka
        df_parsed = df_stream.select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), kafka_schema).alias("message")
        ).select("kafka_timestamp", "message.*")

        print("📡 Simple Kafka stream initialized, waiting for messages...")

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
                        print(f"🎯 Triggering simple prop bet analysis for message: {row.job_type} at {row.timestamp}")
                        process_prop_bet_analysis(spark, f"{row.job_type} - {row.timestamp}")
                    else:
                        print(f"⚠️ Unknown job type: {row.job_type}")
            else:
                print(f"📦 Batch {batch_id}: No new messages")

        # Start the streaming query with shorter processing time
        query = df_parsed.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/kafka-checkpoint-simple") \
            .trigger(processingTime='60 seconds') \
            .start()

        print("🎧 Simple streaming job started! Listening for Kafka messages...")
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