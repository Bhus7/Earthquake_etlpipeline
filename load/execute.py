# load/execute.py

from pathlib import Path
from pyspark.sql import SparkSession

stage_folder = Path("/Users/mac/Earthquake Data/stage_quakes")
eq_path   = stage_folder / "earthquakes"
top6_path = stage_folder / "earthquakes_top6"
daily_path= stage_folder / "earthquakes_daily"

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
db_user  = "postgres"
db_pass  = "postgres"
driver   = "org.postgresql.Driver"

def main():
    spark = (SparkSession.builder
             .appName("QuakeLoad")
             .config("spark.jars.packages","org.postgresql:postgresql:42.7.4")
             .getOrCreate())

    eq_df   = spark.read.parquet(str(eq_path))
    top6_df = spark.read.parquet(str(top6_path))
    daily_df= spark.read.parquet(str(daily_path))

    (eq_df.write.mode("overwrite").format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "earthquakes")
        .option("user", db_user)
        .option("password", db_pass)
        .option("driver", driver)
        .save())

    (top6_df.write.mode("overwrite").format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "earthquakes_top6")
        .option("user", db_user)
        .option("password", db_pass)
        .option("driver", driver)
        .save())

    (daily_df.write.mode("overwrite").format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "earthquakes_daily")
        .option("user", db_user)
        .option("password", db_pass)
        .option("driver", driver)
        .save())

    print("[load] wrote tables: earthquakes, earthquakes_top6, earthquakes_daily")
    spark.stop()

if __name__ == "__main__":
    main()
