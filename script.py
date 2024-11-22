from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg, when, col, lit, desc
from pyspark.sql.types import StringType
import geohash2 as geohash
from dotenv import load_dotenv
import requests
import os

#Initializing SparkSession
#Creates a SparkSession, the entry point for working with PySpark.
#The application is named "RestaurantWeatherAnalysis" for identification
spark = SparkSession.builder.appName("RestaurantWeatherAnalysis").getOrCreate()

#Reading Data
#restaurant_df: Reads restaurant data from a CSV file.
#weather_df: Reads weather data from Parquet files, supporting recursive lookup for nested directories.
restaurant_data_path = "/mnt/c/Users/daniy/Desktop/code/epam/Spark_module/restaurant_csv/restaurant_csv"
weather_data_path = "/mnt/c/Users/daniy/Desktop/code/epam/Spark_module/weather"

#Transforming Data
restaurant_df = spark.read.option("header", "true").csv(restaurant_data_path)
weather_df = spark.read.option("recursiveFileLookup", "true").parquet(weather_data_path)

#Converts latitude (lat) and longitude (lng) columns to double type for numerical operations.
restaurant_df.printSchema()
weather_df.printSchema()

#Repeats this transformation for both restaurant and weather data.
restaurant_df = restaurant_df.withColumn("lat", col("lat").cast("double"))
restaurant_df = restaurant_df.withColumn("lng", col("lng").cast("double"))

weather_df = weather_df.withColumn("lat", col("lat").cast("double"))
weather_df = weather_df.withColumn("lng", col("lng").cast("double"))

#Checking for Missing Values
#Filters and displays rows where lat or lng values are NULL, helping identify incomplete data.
print("Missing data in restaurant_df:")
restaurant_df.filter("lat IS NULL OR lng IS NULL").show()

print("Missing data in weather_df:")
weather_df.filter("lat IS NULL OR lng IS NULL").show()

#Loads the API key from environment variables (dotenv).
load_dotenv()

#External API for Geocoding
#A function to retrieve latitude and longitude for a city and country using the OpenCage API.
def get_coordinates(city, country):
    api_key = os.getenv("OPENCAGE_API_KEY")
    if not api_key:
        raise ValueError("API key for OpenCage is not set in environment variables.")
    url = f"https://api.opencagedata.com/geocode/v1/json?q={city},{country}&key={api_key}"
    response = requests.get(url).json()
    if response['results']:
        lat = response['results'][0]['geometry']['lat']
        lng = response['results'][0]['geometry']['lng']
        return lat, lng
    return None, None

city = "Dillon"
country = "US"
lat, lng = get_coordinates(city, country)

print(f"Coordinates for {city}, {country}: Latitude {lat}, Longitude {lng}")

#Updating Coordinates
#Replaces missing or incorrect coordinates for a specific restaurant (id = 85899345920) with provided latitude and longitude values.
updated_restaurant_df = restaurant_df.withColumn(
    "lat",
    when(col("id") == "85899345920", lit(34.4014089)).otherwise(col("lat"))
).withColumn(
    "lng",
    when(col("id") == "85899345920", lit(-79.3864339)).otherwise(col("lng"))
)

updated_restaurant_df.filter(col("id") == "85899345920").show()

#Generating Geohash
#Defines a function to convert latitude and longitude into a geohash (a compact geographic encoding).
def generate_geohash(lat, lon):
    return geohash.encode(lat, lon, precision=4)

#Applies the function to the restaurant and weather data:
geohash_udf = udf(generate_geohash, StringType())

restaurant_df = updated_restaurant_df.withColumn("geohash", geohash_udf("lat", "lng"))
weather_df = weather_df.withColumn("geohash", geohash_udf("lat", "lng"))

restaurant_df.show(5, truncate=False)
weather_df.show(5, truncate=False)

#Groups weather data by geohash and calculates average temperatures in Celsius and Fahrenheit.
aggregated_weather = weather_df.groupBy("geohash").agg(
    avg("avg_tmpr_c").alias("avg_temperature_c"),
    avg("avg_tmpr_f").alias("avg_temperature_f")
)

aggregated_weather.show(5, truncate=False)

#Joins the restaurant data with aggregated weather data based on geohash.
#The left join ensures all restaurants remain in the dataset, even if no matching weather data exists.
joined_df = restaurant_df.join(aggregated_weather, "geohash", "left")
joined_df.show(5, truncate=False)

joined_df = joined_df.dropDuplicates()

#Data analysis for testing
#Calculate average temperature per restaurant
avg_temp_per_restaurant = joined_df.groupBy("id", "franchise_name", "city").agg(
    avg("avg_temperature_c").alias("average_temperature_c")
)

avg_temp_per_restaurant.show(5, truncate=False)

#Sort restaurants by average temperature
joined_df.orderBy(desc("avg_temperature_c")).show(5, truncate=False)

#Saves the enriched and processed data to a Parquet file at the specified path.
#The overwrite mode ensures that old data is replaced if it exists.
output_path = "/home/dany/code/epam/spark_task/output"
joined_df.write.mode("overwrite").parquet(output_path)