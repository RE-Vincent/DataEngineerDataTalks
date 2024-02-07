-- Table: public.green_tripdata_201909

-- DROP TABLE IF EXISTS public.green_tripdata_201909;

SELECT * FROM green_tripdata_201909
LIMIT 10;

----Question 3
SELECT COUNT(*) FROM green_tripdata_201909
WHERE DATE(lpep_pickup_datetime) = '2019-09-18' AND DATE(lpep_dropoff_datetime) = '2019-09-18';

---Question 4
SELECT CAST(MAX(EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 60) AS NUMERIC(10, 2)) AS minutes,
DATE(lpep_pickup_datetime) AS daystart
FROM green_tripdata_201909
GROUP BY daystart
ORDER BY minutes DESC
LIMIT 1;

---Question 5
WITH ct1
AS
(SELECT 
*
FROM zone zn
JOIN (
	SELECT 
	"PULocationID",
	SUM("total_amount") AS "total_amount"
	FROM
	green_tripdata_201909
	WHERE 
	DATE(lpep_pickup_datetime) = '2019-09-18'
	GROUP BY "PULocationID"
	) gt
ON zn."LocationID" = gt."PULocationID")

SELECT 
SUM(total_amount) AS "total_amount",
"Borough"
FROM ct1
GROUP BY ct1."Borough"
HAVING SUM(total_amount) > 50000;

---Question 6
SELECT 
"lpep_pickup_datetime",
"PULocationID",
"DOLocationID",
"tip_amount",
zn."Zone" AS "ZonePU",
zd."Zone" AS "ZoneDO"
FROM green_tripdata_201909 AS gt
INNER JOIN zone AS zn ON gt."PULocationID" = zn."LocationID"
INNER JOIN zone AS zd ON gt."DOLocationID" = zd."LocationID"
WHERE zn."Zone" = 'Astoria'
ORDER BY gt."tip_amount" DESC
LIMIT 1;



------
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'green_tripdata_201909';