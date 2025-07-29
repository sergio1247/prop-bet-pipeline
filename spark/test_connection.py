from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("TestDBAndJSON") \
    .getOrCreate()

# 1. ✅ Test reading from Postgres
print("Reading from Postgres...")
df_pg = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://pg-player:5432/sportsdb") \
    .option("dbtable", "PlayerStatistics") \
    .option("user", "sergio") \
    .option("password", "yourpassword") \
    .load()

print("Postgres table schema:")
df_pg.printSchema()
print("First few rows from PlayerStatistics:")
df_pg.show(5)

# 2. ✅ Test reading from JSON
print("Reading from JSON...")
df_json = spark.read.json("/app/data/prop_bets.json")

print("Prop bets file schema:")
df_json.printSchema()
print("First few rows from prop_bets.json:")
df_json.show()
