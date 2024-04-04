# Data-Transformation-SQL

Problem Statement:

We want to display on an LED screen, for a set of 5 currency pairs, both the current FX exchange rate and an indication of the change compared to yesterday’s rate at 5PM New York time.
We receive the rates as high frequency (assume updates for multiple currency pairs every millisecond) structured data, similar to the data in rates_sample.csv:

Expected Result:
For the LED screen to display information, it must be fed with input in the below format:
ccy_couple,rate,change
"EUR/USD",1.08081,"-0.208%"

Factual Details:
• event_id: a unique identifier
• event_time: the epoch time in milliseconds
• ccy_couple: the currency pair, made up of the ISO code of two currencies
• rate: the exchange rate value at the given epoch time
• a rate is considered active iff it’s the last one received for a given currency couple AND it’s not older than 30 seconds
• everything not specified is to be decided by you, please document all such decisions
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Requirements & Solution:
A.
We would like to schedule a job which runs every 1 hour to determine the current rate and
percentage change for each of the 5 currency pairs. The job should only take the active rates
into account; for the currency pairs that don’t have an active rate, no output should be
produced.The job should be implemented in the SQL language.

    Solution: Assuming we have a Snowflake account, necessary permission, also a created database and schema to load the data into.
Step 1: We can initially create a table that matches the structure of the CSV file.

CREATE TABLE currency_rates (
    event_id VARCHAR,
    event_time BIGINT,
    ccy_couple VARCHAR(10),
    rate FLOAT
);

Step 2: We can then define the file format.

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

Step 3: We can then create a staging workspace before loading the data to the table.

CREATE OR REPLACE STAGE rates_sample_raw
  FILE_FORMAT = csv_format;

Step 4: Next, we load our generated csv file to staging area using Snowflake CLI

PUT file://our_file_path/rates_sample.csv @rates_sample_raw;

Step 5: We can then copy the data to our table

COPY INTO rates_sample
  FROM @rates_sample_raw/rates_sample.csv
  FILE_FORMAT = (FORMAT_NAME = my_csv_format);

Step 6: Finally verify the loaded data.

SELECT * FROM rates_sample LIMIT 10;

Step 7: Create a task to schedule the job.

Step 8: Start the task.

ALTER TASK Computing_Hourly_Changes RESUME;

