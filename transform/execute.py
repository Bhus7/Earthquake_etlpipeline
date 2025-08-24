# transform/execute.py

from pathlib import Path
from pyspark.sql import SparkSession, functions as F

raw_folder = Path("/Users/mac/Earthquake Data")
stage_folder = Path("/Users/mac/Earthquake Data/stage_quakes")

def latest_csv():
    files = sorted(raw_folder.glob("usgs_*.csv"))
    if not files:
        raise FileNotFoundError("No CSV found in /Users/mac/Earthquake Data. Run extract first.")
    return files[-1]

def main():
    infile = latest_csv()
    print("[transform] reading csv:", infile)
    print("[transform] writing parquet to:", stage_folder)

    spark = (SparkSession.builder
             .appName("QuakeTransform")
             .getOrCreate())

    df = (spark.read.csv(str(infile), header=True, inferSchema=True)
                .withColumn("time", F.to_timestamp("time"))
                .select("id","time","latitude","longitude","depth","mag","place")
                .filter(F.col("time").isNotNull()))

    df = df.withColumn(
        "mag_bucket",
        F.when(F.col("mag") < 3, "M<3")
         .when((F.col("mag") >= 3) & (F.col("mag") < 4), "3–4")
         .when((F.col("mag") >= 4) & (F.col("mag") < 5), "4–5")
         .when((F.col("mag") >= 5) & (F.col("mag") < 6), "5–6")
         .otherwise("6+")
    )
    df = df.withColumn("country", F.trim(F.regexp_extract(F.col("place"), r",\s*([^,]+)$", 1)))

    daily = df.groupBy(F.to_date("time").alias("date")).count().orderBy("date")
    top6  = df.groupBy("country").count().orderBy(F.desc("count")).limit(6)

    # write Parquet to stage
    (stage_folder / "earthquakes").mkdir(parents=True, exist_ok=True)
    (stage_folder / "earthquakes_top6").mkdir(parents=True, exist_ok=True)
    (stage_folder / "earthquakes_daily").mkdir(parents=True, exist_ok=True)

    (df.write.mode("overwrite").parquet(str(stage_folder / "earthquakes")))
    (top6.write.mode("overwrite").parquet(str(stage_folder / "earthquakes_top6")))
    (daily.write.mode("overwrite").parquet(str(stage_folder / "earthquakes_daily")))

    print("[transform] DONE.")
    spark.stop()

if __name__ == "__main__":
    main()