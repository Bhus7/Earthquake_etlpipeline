# transform/execute.py

from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

raw_folder = Path("/Users/mac/Earthquake Data")
stage_folder = Path("/Users/mac/Earthquake Data/stage_quakes")

def latest_csv():
    files = sorted(raw_folder.glob("usgs_*.csv"))
    if not files:
        raise FileNotFoundError("No CSV found in /Users/mac/Earthquake Data. Run extract first.")
    return files[-1]

# simple lat/lon -> country (coarse boxes; student-level is fine)
def country_by_box(lat, lon):
    if lat is None or lon is None: return None
    def inb(a,b,x): return a <= x <= b
    if inb(26.0,31.0,lat) and inb(80.0,89.0,lon): return "Nepal"
    if inb( 6.0,36.0,lat) and inb(68.0,97.5,lon): return "India"
    if inb(18.0,53.6,lat) and inb(73.5,134.8,lon): return "China"
    if inb(23.5,37.1,lat) and inb(60.9,77.5,lon): return "Pakistan"
    if inb(20.5,26.7,lat) and inb(88.0,92.7,lon): return "Bangladesh"
    if inb(26.4,28.3,lat) and inb(88.7,92.1,lon): return "Bhutan"
    if inb( 5.9, 9.9,lat) and inb(79.7,81.9,lon): return "Sri Lanka"
    if inb( 9.4,28.6,lat) and inb(92.2,101.2,lon): return "Myanmar"
    if inb(24.0,46.0,lat) and inb(128.0,146.0,lon): return "Japan"
    if inb(-11.0,6.2,lat) and inb(95.0,141.0,lon): return "Indonesia"
    if inb( 4.5,21.2,lat) and inb(116.9,126.6,lon): return "Philippines"
    if inb(35.8,42.1,lat) and inb(25.7,45.0,lon): return "Turkey"
    if inb(24.0,40.4,lat) and inb(44.0,63.3,lon): return "Iran"
    if inb(-56.0,-17.5,lat) and inb(-76.0,-66.0,lon): return "Chile"
    if inb(14.5,32.7,lat) and inb(-118.6,-86.6,lon): return "Mexico"
    if inb(24.5,49.5,lat) and inb(-125.0,-66.9,lon): return "United States"
    return None

country_by_box_udf = F.udf(country_by_box, T.StringType())

def main():
    infile = latest_csv()
    print("[transform] reading csv:", infile)
    print("[transform] writing parquet to:", stage_folder)

    spark = (SparkSession.builder
             .appName("QuakeTransform")
             .config("spark.driver.memory", "4g")          # add driver memory
             .getOrCreate())

    # 1) read + normalize
    df = spark.read.csv(str(infile), header=True, inferSchema=True)
    df = df.replace(["", "null", "NULL", "NaN", "nan"], None)

    # 2) cast + select
    df = (df
          .withColumn("time",      F.to_timestamp("time"))
          .withColumn("latitude",  F.col("latitude").cast("double"))
          .withColumn("longitude", F.col("longitude").cast("double"))
          .withColumn("depth",     F.col("depth").cast("double"))
          .withColumn("mag",       F.col("mag").cast("double"))
          .select("id", "time", "latitude", "longitude", "depth", "mag", "place"))

    # 3) drop rows that break charts
    df = df.filter(
        F.col("time").isNotNull() &
        F.col("latitude").isNotNull() &
        F.col("longitude").isNotNull() &
        F.col("mag").isNotNull()
    ).filter(
        (F.col("latitude") >= -90) & (F.col("latitude") <= 90) &
        (F.col("longitude") >= -180) & (F.col("longitude") <= 180)
    )

    # 4) mag bucket
    df = df.withColumn(
        "mag_bucket",
        F.when(F.col("mag") < 3, "M<3")
         .when((F.col("mag") >= 3) & (F.col("mag") < 4), "3–4")
         .when((F.col("mag") >= 4) & (F.col("mag") < 5), "4–5")
         .when((F.col("mag") >= 5) & (F.col("mag") < 6), "5–6")
         .otherwise("6+")
    )

    # 5) country: comma parse → keyword → lat/lon UDF
    country_from_comma = F.trim(F.regexp_extract(F.col("place"), r",\s*([^,]+)$", 1))
    place_lower = F.lower(F.coalesce(F.col("place"), F.lit("")))

    country_kw = (
        F.when(place_lower.like("%nepal%"),        F.lit("Nepal"))
         .when(place_lower.like("%india%"),        F.lit("India"))
         .when(place_lower.like("%china%"),        F.lit("China"))
         .when(place_lower.like("%pakistan%"),     F.lit("Pakistan"))
         .when(place_lower.like("%bangladesh%"),   F.lit("Bangladesh"))
         .when(place_lower.like("%bhutan%"),       F.lit("Bhutan"))
         .when(place_lower.like("%sri lanka%"),    F.lit("Sri Lanka"))
         .when(place_lower.like("%myanmar%"),      F.lit("Myanmar"))
         .when(place_lower.like("%japan%"),        F.lit("Japan"))
         .when(place_lower.like("%indonesia%"),    F.lit("Indonesia"))
         .when(place_lower.like("%philippines%"),  F.lit("Philippines"))
         .when(place_lower.like("%turkey%"),       F.lit("Turkey"))
         .when(place_lower.like("%iran%"),         F.lit("Iran"))
         .when(place_lower.like("%chile%"),        F.lit("Chile"))
         .when(place_lower.like("%mexico%"),       F.lit("Mexico"))
         .when(place_lower.like("%united states%"),F.lit("United States"))
    )

    df = df.withColumn("country_guess_box", country_by_box_udf(F.col("latitude"), F.col("longitude")))
    df = df.withColumn(
        "country",
        F.when(F.length(country_from_comma) > 0, country_from_comma)
         .when(country_kw.isNotNull(),            country_kw)
         .when(F.col("country_guess_box").isNotNull(), F.col("country_guess_box"))
         .otherwise(F.lit("Unknown"))
    ).drop("country_guess_box")

    # 6) remove Unknown completely (so PG/Superset never show it)
    df_no_unknown = df.filter(F.col("country") != "Unknown")

    # 7) aggregates
    daily = (df_no_unknown.groupBy(F.to_date("time").alias("date"))
               .count()
               .orderBy("date"))

    top6 = (df_no_unknown.groupBy("country")
              .count()
              .orderBy(F.desc("count"))
              .limit(6))

    # 8) write parquet
    (stage_folder / "earthquakes").mkdir(parents=True, exist_ok=True)
    (stage_folder / "earthquakes_top6").mkdir(parents=True, exist_ok=True)
    (stage_folder / "earthquakes_daily").mkdir(parents=True, exist_ok=True)

    df_no_unknown.write.mode("overwrite").parquet(str(stage_folder / "earthquakes"))
    top6.write.mode("overwrite").parquet(str(stage_folder / "earthquakes_top6"))
    daily.write.mode("overwrite").parquet(str(stage_folder / "earthquakes_daily"))

    print("[transform] DONE (OOM-safe; Unknown removed).")
    spark.stop()

if __name__ == "__main__":
    main()
