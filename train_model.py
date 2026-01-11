from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, when
import glob
import time
import os

# Initialize Spark 
spark = SparkSession.builder \
    .appName("FloodModelAutoRetrainer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

HISTORY_FILE = "real_flood_history.csv"
LIVE_DATA_PATH = "./live_data_storage/*.json"
MODEL_PATH = "flood_logistic_model"

def train_cycle():
    print(f"🔄 [Auto-Retrainer] Waking up to update model...")

    # load baseline historical data
    try:
        df_history = spark.read.csv(HISTORY_FILE, header=True, inferSchema=True)
        # Extra data for Portland removed
        df_history = df_history.filter(df_history.city != "Portland") \
                               .select("level_percent", "rain_1h", "rain_24h", "label")
    except:
        print("! No historical baseline found.")
        df_history = None

    # load live data
    df_new = None
    try:
        if glob.glob(LIVE_DATA_PATH):
            raw_new = spark.read.json(LIVE_DATA_PATH)
            
            # Feature Engineering on Raw Data
            # level_percent = water_level / flood_threshold
            df_new = raw_new.withColumn("level_percent", raw_new.water_level / raw_new.flood_threshold)
            
            # self labeling, if the level is above 95%, label as 1.0 (Danger)
            df_new = df_new.withColumn("label", when(col("level_percent") > 0.95, 1.0).otherwise(0.0))
            
            df_new = df_new.select("level_percent", "rain_1h", "rain_24h", "label")
            print(f"   found {df_new.count()} new live records.")
    except Exception as e:
        print(f"   (No live data yet: {e})")

    # 3. COMBINE
    if df_history is not None and df_new is not None:
        training_data = df_history.union(df_new)
    elif df_history is not None:
        training_data = df_history
    else:
        print(" !!! No data. Sleeping.")
        return

    # 4. TRAIN
    assembler = VectorAssembler(
        inputCols=["level_percent", "rain_1h", "rain_24h"],
        outputCol="features"
    )
    final_data = assembler.transform(training_data)
    
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    model = lr.fit(final_data)
    
    # 5. SAVE (Overwrite)
    model.write().overwrite().save(MODEL_PATH)
    print(f"Model Updated! Next cycle in 5 minutes.")

# --- MAIN LOOP ---
if __name__ == "__main__":
    print("Model Started.")
    while True:
        train_cycle()
        time.sleep(300) # Sleep for 300 seconds (5 minutes)