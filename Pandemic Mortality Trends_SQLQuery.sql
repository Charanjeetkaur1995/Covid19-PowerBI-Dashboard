
-- Query 1 Retrieve the jurisdiction residence with the highest number of COVID deaths for the latest  data period end date--

QUERY:
WITH Latest_Period AS (
    SELECT MAX(data_period_end) AS latest_date FROM da_data_1
)
SELECT Jurisdiction_Residence, COVID_deaths
FROM da_data_1
WHERE data_period_end = (SELECT latest_date FROM Latest_Period)
ORDER BY COVID_deaths DESC
LIMIT 1;


OUTPUT:
Query returned 
United States	1092158


-- Query 2 Retrieve the top 5 jurisdictions with the highest percentage difference in aa_COVID_rate  
-- compared to the overall crude COVID rate for the latest data period end date.--
QUERY:
SELECT 
Jurisdiction_Residence, 
aa_COVID_rate, 
crude_COVID_rate,
((aa_COVID_rate - crude_COVID_rate) / crude_COVID_rate) * 100 AS pct_difference
FROM `da_data_1`
WHERE STR_TO_DATE(data_period_end, '%d-%m-%Y') = (
SELECT MAX(STR_TO_DATE(data_period_end, '%d-%m-%Y'))
FROM `da_data_1`
)
ORDER BY pct_difference DESC
LIMIT 5;

OUTPUT :

Jurisdiction_Residence, aa_COVID_rate, crude_COVID_rate, pct_difference

Utah	193	162	19.1358
Alaska	220	202	8.9109
Texas	351	342	2.6316
District of Columbia	278	276	0.7246
Region 3	1	1	0.0000



-- Query 3 Calculate the average COVID deaths per week for each jurisdiction residence and group, 
-- for  the latest 4 data period end dates.

QUERY:
WITH Latest_Periods AS (
SELECT DISTINCT STR_TO_DATE(data_period_end, '%d-%m-%Y') AS period_end
FROM `da_data_1`
ORDER BY period_end DESC
LIMIT 4)
SELECT 
c.Jurisdiction_Residence, 
c.Group,
SUM(c.COVID_deaths) / SUM(DATEDIFF(STR_TO_DATE(c.data_period_end, '%d-%m-%Y'), 
STR_TO_DATE(c.data_period_start, '%d-%m-%Y')) / 7) 
AS avg_weekly_deaths
FROM `da_data_1` c
JOIN Latest_Periods lp 
ON STR_TO_DATE(c.data_period_end, '%d-%m-%Y') = lp.period_end
WHERE c.COVID_deaths IS NOT NULL
GROUP BY c.Jurisdiction_Residence, c.Group
ORDER BY avg_weekly_deaths DESC;

OUTPUT:

Jurisdiction_Residence, Group, avg_weekly_deaths

United States	total	5774.8283
Region 4	total	1297.1593
Region 5	total	944.2442
Region 6	total	821.4333
Region 9	total	771.4815
Region 2	total	623.2506



-- Query 4 Retrieve the data for the latest data period end date,
--  but exclude any jurisdictions that had  zero COVID deaths and have missing values in any other column.
QUERY :
SELECT *
FROM da_data_1
WHERE data_period_end = (SELECT MAX(data_period_end) FROM da_data_1)  -- Latest data period
AND COVID_deaths > 0  -- Exclude zero COVID deaths
AND (COVID_pct_of_total IS NOT NULL AND pct_change_wk IS NOT NULL AND pct_diff_wk IS NOT NULL 
AND crude_COVID_rate IS NOT NULL AND aa_COVID_rate IS NOT NULL);  -- Exclude missing values

OUTPUT :
data_as_of, Jurisdiction_Residence, Group, data_period_start, data_period_end, COVID_deaths, COVID_pct_of_total, pct_change_wk, pct_diff_wk, crude_COVID_rate, aa_COVID_rate, footnote


04-12-2023	Region 1	total	01-01-2020	12/31/2022	42432	10			281	212	
04-12-2023	Region 2	total	01-01-2020	12/31/2022	117818	13			364	273	
04-12-2023	Region 3	total	01-01-2020	12/31/2022	103134	10			330	260	
04-12-2023	Region 4	total	01-01-2020	12/31/2022	244938	11			361	284	
04-12-2023	Region 5	total	01-01-2020	12/31/2022	178712	10			338	274	
04-12-2023	Region 6	total	01-01-2020	12/31/2022	155874	12			360	346	
04-12-2023	Region 7	total	01-01-2020	12/31/2022	47881	10			336	270	


QUERY :
-- Query 5 Calculate the week-over-week percentage change in COVID_pct_of_total 
-- for all jurisdictions  and groups, but only for the data period start dates after March 1, 2020.

-- Steps to convert date in correct format  to solve the query 
--
DESC da_data_1;
SELECT DISTINCT data_period_start FROM da_data_1 LIMIT 10;
select COUNT(*) 
FROM da_data_1 
WHERE STR_TO_DATE(data_period_start, '%m-%d-%Y') > '2020-03-01';


WITH Filtered_Data AS (
    SELECT 
        Jurisdiction_Residence,
        `Group`,
        STR_TO_DATE(data_period_start, '%m-%d-%Y') AS data_period_start,
        data_period_end,
        COVID_pct_of_total,
        LAG(COVID_pct_of_total) OVER (
            PARTITION BY Jurisdiction_Residence, `Group`
            ORDER BY STR_TO_DATE(data_period_start, '%m-%d-%Y')
        ) AS previous_week_pct
    FROM da_data_1
    WHERE STR_TO_DATE(data_period_start, '%m-%d-%Y') > '2020-03-01'
)
SELECT 
    Jurisdiction_Residence,
    `Group`,
    data_period_start,
    data_period_end,
    COVID_pct_of_total,
    previous_week_pct,
    CASE 
        WHEN previous_week_pct IS NOT NULL AND previous_week_pct != 0
        THEN ((COVID_pct_of_total - previous_week_pct) / previous_week_pct) * 100
        ELSE NULL
    END AS week_over_week_pct_change
FROM Filtered_Data;


OUTPUT :

Jurisdiction_Residence, Group, data_period_start, data_period_end, COVID_pct_of_total, previous_week_pct, week_over_week_pct_change

Alabama	weekly	2020-03-08	03/14/2020	0		
Alabama	weekly	2020-04-05	04-11-2020	6	0	
Alabama	weekly	2020-04-12	04/18/2020	8	6	33.3333
Alabama	weekly	2020-05-03	05-09-2020	10	8	25.0000
Alabama	weekly	2020-05-10	05/16/2020	9	10	-10.0000
Alabama	weekly	2020-06-07	06/13/2020	7	9	-22.2222
Alabama	weekly	2020-07-05	07-11-2020	11	7	57.1429
Alabama	weekly	2020-07-12	07/18/2020	15	11	36.3636
Alabama	weekly	2020-08-02	08-08-2020	20	15	33.3333
Alabama	weekly	2020-08-09	08/15/2020	18	20	-10.0000

QUERY :
-- Query 6 Group the data by jurisdiction residence and calculate 
-- the cumulative COVID deaths for each  jurisdiction, but only up to the latest data period end date.
WITH LatestDate AS (
-- Get the latest data_period_end date in the dataset
SELECT MAX(STR_TO_DATE(data_period_end, '%m/%d/%Y')) AS max_date FROM da_data_1)
SELECT 
d.Jurisdiction_Residence,
d.data_period_end,
d.COVID_deaths,
SUM(d.COVID_deaths) OVER (
PARTITION BY d.Jurisdiction_Residence 
ORDER BY STR_TO_DATE(d.data_period_end, '%m/%d/%Y')
) AS cumulative_COVID_deaths
FROM da_data_1 d
JOIN LatestDate ld
ON STR_TO_DATE(d.data_period_end, '%m/%d/%Y') <= ld.max_date
ORDER BY d.Jurisdiction_Residence, STR_TO_DATE(d.data_period_end, '%m/%d/%Y');
OUTPUT :

Jurisdiction_Residence, data_period_end, COVID_deaths, cumulative_COVID_deaths

'Alabama', '03/28/2020', '24', '53'
'Alabama', '03/28/2020', '29', '53'
'Alabama', '04/18/2020', '239', '383'
'Alabama', '04/18/2020', '91', '383'
'Alabama', '04/25/2020', '86', '794'
'Alabama', '04/25/2020', '325', '794'
'Alabama', '05/16/2020', '625', '1520'
'Alabama', '05/16/2020', '101', '1520'
'Alabama', '05/23/2020', '87', '2319'
'Alabama', '05/23/2020', '712', '2319'
'Alabama', '05/30/2020', '823', '3253'
'Alabama', '05/30/2020', '111', '3253'
'Alabama', '06/13/2020', '85', '4337'


SQL Analysis:
Implementation of Function & Procedure-"Create a stored procedure that takes in a date  range and 
calculates the average weekly percentage change in COVID deaths for each  jurisdiction. 
The procedure should return the average weekly percentage change along with  the jurisdiction and date range as output.
 Additionally, create a user-defined function that  takes in a jurisdiction as input and returns 
 the average crude COVID rate for that jurisdiction  over the entire dataset. Use both the stored procedure and the 
 user-defined function to  compare the average weekly percentage change in COVID deaths for each jurisdiction to the 
 average crude COVID rate for that jurisdiction.
 
-- Create a table to store weekly percentage change results
CREATE TABLE IF NOT EXISTS Weekly_Change (
    jurisdiction VARCHAR(255),
    date_range_start DATE,
    date_range_end DATE,
    avg_weekly_percentage_change DECIMAL(12,4)
);

-- Create the stored procedure to calculate average weekly percentage change
DELIMITER $$
CREATE PROCEDURE Get_Avg_Weekly_Percentage_Change(
    IN startDate DATE,
    IN endDate DATE
)
BEGIN
    -- Clear previous data to avoid duplication
    TRUNCATE TABLE Weekly_Change;
    
    -- Insert new aggregated data
    INSERT INTO Weekly_Change (jurisdiction, date_range_start, date_range_end, avg_weekly_percentage_change)
    SELECT
        Jurisdiction_Residence AS jurisdiction,
        startDate AS date_range_start,
        endDate AS date_range_end,
        ROUND(AVG(pct_change_wk), 4) AS avg_weekly_percentage_change
    FROM DA_Data
    WHERE 
        (
            (data_period_start REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' AND STR_TO_DATE(data_period_start, '%m/%d/%Y') BETWEEN startDate AND endDate)
            OR 
            (data_period_start REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' AND STR_TO_DATE(data_period_start, '%d-%m-%Y') BETWEEN startDate AND endDate)
        )
    GROUP BY Jurisdiction_Residence;
END$$
DELIMITER ;


-- Create the user-defined function to get the average crude COVID rate
DELIMITER $$
CREATE FUNCTION Get_Avg_Crude_COVID_Rate(jurisdiction_name VARCHAR(255))
RETURNS DECIMAL(12,4)
DETERMINISTIC
BEGIN
    DECLARE avg_crude_rate DECIMAL(12,4);
    SELECT AVG(crude_COVID_rate)
    INTO avg_crude_rate
    FROM DA_Data
    WHERE Jurisdiction_Residence = jurisdiction_name;
    RETURN avg_crude_rate;
END$$
DELIMITER ;



-- Execute stored procedure and use the result for comparison
CALL Get_Avg_Weekly_Percentage_Change('2020-01-01', '2023-12-31');

SELECT
    w.jurisdiction,
    w.avg_weekly_percentage_change,
    Get_Avg_Crude_COVID_Rate(w.jurisdiction) AS avg_crude_covid_rate
FROM Weekly_Change w;























-- Create a table to store weekly percentage change results
CREATE TABLE IF NOT EXISTS Weekly_Change (
    jurisdiction VARCHAR(255),
    date_range_start DATE,
    date_range_end DATE,
    avg_weekly_percentage_change DECIMAL(12,4)
);

-- Create the stored procedure to calculate average weekly percentage change
DELIMITER $$
CREATE PROCEDURE Get_Avg_Weekly_Percentage_Change(
    IN startDate DATE,
    IN endDate DATE
)
BEGIN
    -- Clear previous data to avoid duplication
    TRUNCATE TABLE Weekly_Change;
    
    -- Insert new aggregated data
    INSERT INTO Weekly_Change (jurisdiction, date_range_start, date_range_end, avg_weekly_percentage_change)
    SELECT
        Jurisdiction_Residence AS jurisdiction,
        startDate AS date_range_start,
        endDate AS date_range_end,
        ROUND(AVG(pct_change_wk), 4) AS avg_weekly_percentage_change
    FROM DA_Data
    WHERE 
        (
            (data_period_start REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' AND STR_TO_DATE(data_period_start, '%m/%d/%Y') BETWEEN startDate AND endDate)
            OR 
            (data_period_start REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' AND STR_TO_DATE(data_period_start, '%d-%m-%Y') BETWEEN startDate AND endDate)
        )
    GROUP BY Jurisdiction_Residence;
END$$
DELIMITER ;


-- Create the user-defined function to get the average crude COVID rate
DELIMITER $$
CREATE FUNCTION Get_Avg_Crude_COVID_Rate(jurisdiction_name VARCHAR(255))
RETURNS DECIMAL(12,4)
DETERMINISTIC
BEGIN
    DECLARE avg_crude_rate DECIMAL(12,4);
    SELECT AVG(crude_COVID_rate)
    INTO avg_crude_rate
    FROM DA_Data
    WHERE Jurisdiction_Residence = jurisdiction_name;
    RETURN avg_crude_rate;
END$$
DELIMITER ;



-- Execute stored procedure and use the result for comparison
CALL Get_Avg_Weekly_Percentage_Change('2020-01-01', '2023-12-31');

SELECT
    w.jurisdiction,
    w.avg_weekly_percentage_change,
    Get_Avg_Crude_COVID_Rate(w.jurisdiction) AS avg_crude_covid_rate
FROM Weekly_Change w;
