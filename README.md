### 1. What is the monthly count of unique Users (headcount) who have made a booking for the last 6 months?

```sql
SELECT DATE_TRUNC('month', b.booking_start_time) AS month,
       COUNT(DISTINCT u.id) AS headcount
FROM bookings b
JOIN users u ON b.user_id = u.id
WHERE b.booking_start_time IS NOT NULL AND b.booking_end_time IS NOT NULL AND
	(b.booking_start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months'
  AND b.booking_start_time < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')
GROUP BY DATE_TRUNC('month', b.booking_start_time)
ORDER BY month;
```

| month               | headcount |
| ------------------- | --------- |
| 2022-12-01 00:00:00 | 175       |
| 2023-01-01 00:00:00 | 176       |
| 2023-02-01 00:00:00 | 183       |
| 2023-03-01 00:00:00 | 204       |
| 2023-04-01 00:00:00 | 196       |
| 2023-05-01 00:00:00 | 160       |

### 2. How many users need more than 30 days to make their first booking, and from which company are those users ?

```sql
WITH first_booking AS (
                      SELECT rn, company_id, u.created_at, MIN(b.booking_start_time) first_booking
                      FROM bookings b
                      JOIN users u
                      ON b.user_id = u.rn
                      WHERE u.created_at IS NOT NULL AND b.booking_start_time IS NOT NULL
                      GROUP BY rn, u.created_at, company_id
                      )
SELECT c.company_name, COUNT(*) AS user_count
FROM first_booking fb
JOIN companies c
ON c.company_id = fb.company_id
WHERE fb.first_booking > fb.created_at + INTERVAL '30 days'
GROUP BY c.company_name;
```

| company_name         | user_count |
| -------------------- | ---------- |
| Deskbooking GmBH     | 1          |
| We love Deskbooking  | 8          |
| Prime Deskbooker     | 46         |
| BookerCentrics       | 64         |
| Flexbooking Partners | 81         |

### 3. What is the daily 7 day rolling total booking amount for March 2023?

```sql
SELECT booking_date, SUM(booking_count) OVER (ORDER BY booking_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_total
FROM (
    SELECT DATE_TRUNC('day', booking_start_time) AS booking_date, COUNT(*) AS booking_count
    FROM bookings
    WHERE booking_start_time >= DATE '2023-03-01' AND booking_start_time < DATE '2023-04-01'
    GROUP BY DATE_TRUNC('day', booking_start_time)
) AS subquery
ORDER BY booking_date;
```

| booking_date        | rolling_total |
| ------------------- | ------------- |
| 2023-03-01 00:00:00 | 76            |
| 2023-03-02 00:00:00 | 150           |
| 2023-03-03 00:00:00 | 189           |
| 2023-03-06 00:00:00 | 241           |
| 2023-03-07 00:00:00 | 314           |
| 2023-03-08 00:00:00 | 401           |
| 2023-03-09 00:00:00 | 466           |
| 2023-03-10 00:00:00 | 433           |
| 2023-03-13 00:00:00 | 416           |
| 2023-03-14 00:00:00 | 466           |
| 2023-03-15 00:00:00 | 546           |
| 2023-03-16 00:00:00 | 561           |
| 2023-03-17 00:00:00 | 526           |
| 2023-03-20 00:00:00 | 508           |
| 2023-03-21 00:00:00 | 552           |
| 2023-03-22 00:00:00 | 571           |
| 2023-03-23 00:00:00 | 543           |
| 2023-03-24 00:00:00 | 437           |
| 2023-03-27 00:00:00 | 394           |
| 2023-03-28 00:00:00 | 435           |
| 2023-03-29 00:00:00 | 469           |
| 2023-03-30 00:00:00 | 468           |
| 2023-03-31 00:00:00 | 429           |

### Which principles should you follow when building pipelines?

The are a few principles that I would follow when building pipelines:

- The pipeline should be scalable.
- The pipeline should be incremental and idempotent. This would allow us to run the pipeline multiple times without duplicating the data.
- The pipeline should be easy to maintain and debug.
- The pipeline should be easy to test.
- The pipeline should be secure.

# Which are there any anti-patterns you would avoid?

There are a few anti-patterns that I would avoid:

- I would avoid using the raw data as the final data for analysis.
- I would avoid using a single script/function/pipeline to do all the work. We should keep our code clean and simple.
- I would avoid having sensitive data in the data warehouse. It should be encripted or masked.
- I would avoid having a single point of failure. The pipeline should be able to recover from failures.
- I would avoid having a pipeline that runs manually. It should be scheduled to run automatically.
- I would avoing having a pipeline that inserts duplicates if it run multiple times.

### How would you improve your current solution further, if you had more time?

- The docker compose file running the postgres & pgadmin containers could be improved to bundle up python and the load data in batch scripts.
- Credentials for the user accounts should be stored in a more secure location/environment file although docker-compose.yml may be enough.
- Passwords should be harder to brute-force and more secure.
- In order to import the files the user needs to manually run docker cp into the postgres container, this can be automated with docker library command or with a custom build script in docker-compose.yml.
- For this task I would use a data pipeline tool like Airflow or Luigi. The pipeline would be scheduled to run daily and would use incremental data updates.
- To make sure the data is ingested in its original form, I would use a raw layer. After that, I would use a different pipeline to clean and transform the data before loading it into another layer. The analysis can be performed on the final layer.

## Steps to run the assignment solution

### 1. Install docker if not installed

### 2. Run command to start the containers:

```bash
-- install deps
pip install -r requirements.txt

-- start containers
docker compose up

-- for containerID
docker container ls

-- open container to debug
docker exec -it <containerId> bash

-- manually copy csvs to pg instance
docker cp .\output\ <containerId>:/output/
```

### Notes:

The copy command to import csv s to db may not work on C drive due to user permissions, so I had to copy files to postgres container followed by import script.

### Testing

I used pytest to test the ingestion data processes,

`test_x_ingestion` checks that the number of rows in the x table is the same as the number of rows in the CSV file, x being users, bookings or company.

```python

`test_bookings_row_by_row`, validates the accuracy of individual rows in a CSV file containing booking data. It reads the CSV file into a DataFrame, sorts it by specific columns, and iterates over each row. For each row, it executes a SQL query to retrieve the corresponding data from a database. The test then compares the values from the DataFrame row with the retrieved database values, ensuring they match for various fields.
```
