Project Overview

This script retrieves real-time weather data for a set of cities using their latitude and longitude coordinates stored in a Snowflake table. The fetched data is processed and saved both as a CSV file in Databricks DBFS and in a Snowflake table for further analysis and use.

---

Features

Integration with Snowflake:
Reads city coordinates (latitude and longitude) from a Snowflake table named PLACES_LAT_LONG.
Weather Data Retrieval:
Uses the WeatherAPI to fetch real-time weather details such as temperature, humidity, and weather conditions.
Data Processing:
Converts the retrieved data into a structured format using PySpark.

Data Storage:

Saves the processed weather data:
1. As a CSV file in Databricks File System (DBFS).
2. As a new table (Weather_Live_Date) in Snowflake.
---

Prerequisites

1. Databricks Environment:
A Databricks workspace with access to Spark.
2. Snowflake Setup:
A Snowflake database containing the PLACES_LAT_LONG table with the following structure:
SERIAL_NO (Integer): Unique identifier for each city.
PLACE (String): City name.
LATITUDE (Float): Latitude of the city.
LONGITUDE (Float): Longitude of the city.
3. WeatherAPI Key:
Obtain an API key from WeatherAPI.
---

Setup Instructions

1. Install Required Libraries: Ensure the following libraries are installed in your Databricks environment:
pyspark
requests
snowflake-connector-python
snowflake-sqlalchemy (if required)

2. Configure Snowflake Credentials: Update the following placeholders in the script with your Snowflake credentials:

snowflake_options = {
"sfURL": "EJDJBCL-ZI07581.snowflakecomputing.com",
    "sfDatabase": "PLACES_COORDS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfUser": "your_username",
    "sfPassword": "your_password"
    }

3. Add WeatherAPI Key: Replace the placeholder with your WeatherAPI key:
api_key = "your_api_key"

4. Verify Data in Snowflake: Ensure the PLACES_LAT_LONG table exists and contains valid city coordinates.
5. Setup DBFS Mount (Optional): If required, ensure a mount point is configured for DBFS to save the output.
---

Script Details

1. Snowflake Integration
Reads city data (SERIAL_NO, PLACE, LATITUDE, LONGITUDE) from the PLACES_LAT_LONG table.

2. API Integration
Fetches real-time weather data from the WeatherAPI using latitude and longitude as input.

3. Data Processing
Processes the API response and structures the data into a PySpark DataFrame with the following schema:

S.No: Serial number of the city.
City: Name of the city.
Time Stamp: Last updated time of the weather data.
Temperature (°C): Temperature in Celsius.
Humidity (%): Humidity percentage.
Weather Condition: Textual description of the weather condition.

4. Data Storage

Saves the weather data:
1. As a CSV file in DBFS (dbfs:/mnt/data/weather_data).
2. In a Snowflake table (Weather_Live_Date).
---

How to Run the Script
1. Upload the Script to Databricks:
Copy and paste the script into a Databricks notebook.
2. Run the Script:
Execute the notebook in your Databricks cluster.
3. Verify Outputs:
Check the CSV output in DBFS at dbfs:/mnt/data/weather_data.
Validate the data in the Weather_Live_Date table in Snowflake.
---

Expected Output

1. CSV File:
A CSV file named weather_data.csv containing real-time weather data for all cities.

2. Snowflake Table:
A table named Weather_Live_Date with the processed weather data.
---

Notes

Ensure the PLACES_LAT_LONG table in Snowflake has valid coordinates.
If any API calls fail, the corresponding city data will not be included in the output. Check the logs for details.
Modify the .mode("overwrite") to .mode("append") if you want to keep historical weather data in the Snowflake table.

---

Troubleshooting

1. Snowflake Connection Issues:
Verify Snowflake credentials and network access to the Snowflake server.
Ensure the snowflake-connector-python library is installed in your Databricks cluster.

2. API Errors:
Ensure the WeatherAPI key is valid and has sufficient quota.
Check for rate limits or invalid coordinates.

4. DBFS Issues:
Confirm the dbfs:/mnt/data/ path is accessible in your Databricks environment.
---

