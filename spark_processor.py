from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler

# Initialize
spark = SparkSession.builder \
    .appName("FloodWatchStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Schema
schema = StructType([
    StructField("timestamp", DoubleType()),
    StructField("location_id", StringType()),
    StructField("location_name", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("water_level", DoubleType()),
    StructField("flood_threshold", DoubleType()),
    StructField("rain_1h", DoubleType()),
    StructField("rain_24h", DoubleType())
])

# Read Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flood_data") \
    .load()

parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty(): return

    # reload dynamic model (Hot Swapping)
    try:
        model = LogisticRegressionModel.load("flood_logistic_model")
    except:
        print("Waiting for model...")
        return

    # feature engineering
    processed_df = batch_df.withColumn("level_percent", col("water_level") / col("flood_threshold"))
    
    assembler = VectorAssembler(inputCols=["level_percent", "rain_1h", "rain_24h"], outputCol="features")
    vector_df = assembler.transform(processed_df)
    
    # prediction
    predictions = model.transform(vector_df)
    
    final_df = predictions.withColumn(
        "risk_status",
        when(col("prediction") == 1.0, "DANGER").otherwise("SAFE")
    )

    # Save outputs
    # For Dashboard
    final_df.select("location_name", "lat", "lon", "water_level", "rain_1h", "rain_24h", "risk_status") \
        .write.mode("append").json("./prediction_output")
        
    # For Retraining Loop, Save raw data
    batch_df.select("water_level", "flood_threshold", "rain_1h", "rain_24h") \
        .write.mode("append").json("./live_data_storage")

    print(f"Batch {batch_id} processed!!!!!!!")

query = parsed_df.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()
