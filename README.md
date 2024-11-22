# Restaurant Weather Analysis

## Overview
This project integrates restaurant and weather data using **PySpark** for analysis and geospatial processing. 
By combining restaurant location data with aggregated weather information, we gain insights into temperature trends for restaurants. 
The project also demonstrates integration with external APIs for geocoding missing coordinates.

---

## Features
1. **Data Cleaning and Transformation**:
   - Reads restaurant data from CSV and weather data from Parquet.
   - Handles missing or incorrect latitude/longitude values.

2. **Integration with External API**:
   - Uses the OpenCage Geocoder API to fetch geographical coordinates for missing data.

3. **Geospatial Aggregation**:
   - Generates **geohashes** for spatial grouping.
   - Aggregates weather data (average temperatures in Celsius and Fahrenheit) for each geohash.

4. **Data Enrichment**:
   - Joins enriched weather data with restaurant data based on geohash.

5. **Test and Analysis**:
   - Calculates average temperature for each restaurant.
   - Provides sorted lists of restaurants based on temperature trends.

6. **Export Results**:
   - Saves enriched data in **Parquet format** for further processing or analysis.


## Requirements
- Python 3.8+
- PySpark
- geohash2
- requests
- python-dotenv


## How It Works

**Data Loading**
   -Reads restaurant data from restaurant_csv and weather data from weather directory.
   -Ensures latitude and longitude are numeric.

**Missing Data Handling**
   -Identifies missing latitude/longitude values.
   -For specific restaurants, updates missing coordinates using hardcoded replacements.
   -Retrieves missing coordinates via the OpenCage API if not already available.

**Geohash Generation**
   -Converts latitude and longitude into geohashes for spatial aggregation.

**Weather Aggregation**
   -Groups weather data by geohash.
   -Calculates average temperature for each geohash.

**Data Enrichment**
   -Joins aggregated weather data with restaurant data based on geohash.

**Insights and Analysis**
   -Calculates average temperature for each restaurant.
   -Sorts restaurants based on their average temperatures.

**Save Results**
   -Saves the final enriched dataset as a Parquet file in the output/ directory.

## Key Functions in the Code

**Data Cleaning and Transformation**
   -withColumn: Updates data types for latitude and longitude.
   -filter: Identifies rows with missing data.

**API Integration**
   -get_coordinates(city, country): Fetches coordinates for a city and -country from OpenCage API.

**Geohash Generation**
   -generate_geohash(lat, lon): Encodes latitude and longitude into a -geohash.

**Data Test and analysis**
   -groupBy and agg: Aggregates data for weather and restaurants.
   -orderBy: Sorts restaurants based on temperature trends.
   
   ![alt text](<Screenshot 2024-11-22 184231.png>)
   ![alt text](<Screenshot 2024-11-22 184208.png>)