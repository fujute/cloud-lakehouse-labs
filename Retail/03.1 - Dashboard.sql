-- Databricks notebook source
-- MAGIC %python
-- MAGIC catalogName='main'
-- MAGIC databaseName='f11l_databricks_com_retail'
-- MAGIC spark.sql("use catalog "+catalogName)
-- MAGIC spark.sql("use database "+databaseName)

-- COMMAND ----------

-- 1. Total MRR 
-- Create a counter visualisation
SELECT
  sum(amount)/1000 as MRR
FROM churn_orders
WHERE
    month(to_timestamp(transaction_date, 'MM-dd-yyyy HH:mm:ss')) = 
  (
    select max(month(to_timestamp(transaction_date, 'MM-dd-yyyy HH:mm:ss')))
      from churn_orders
  );


-- COMMAND ----------

-- 2.MRR at MRR at risk 
-- Create a counter visualisation

SELECT
    sum(amount)/1000 as MRR_at_risk
FROM churn_orders
WHERE month(to_timestamp(transaction_date, 'MM-dd-yyyy HH:mm:ss')) = 
    (
        select max(month(to_timestamp(transaction_date, 'MM-dd-yyyy HH:mm:ss')))
        from churn_orders
    )
    and user_id in
    (
        SELECT user_id FROM churn_prediction WHERE churn_prediction=1
    )




-- COMMAND ----------

-- 3. Customer at Risk
SELECT count(*) as Customers, cast(churn_prediction as boolean) as `At Risk`
FROM churn_prediction GROUP BY churn_prediction;




-- COMMAND ----------

-- For 4 and 5 switch to the schema where the DLT tables are created
-- 4. Customer Tenure - Historical
-- Create a bar visualisation
SELECT cast(days_since_creation/30 as int) as days_since_creation, churn, count(*) as customers
FROM churn_features
GROUP BY days_since_creation, churn having days_since_creation < 1000




-- COMMAND ----------

-- 5. Subscriptions by Internet Service - Historical
-- Create a horizontal bar visualisation
select platform, churn, count(*) as event_count
from churn_app_events
inner join churn_users using (user_id)
where platform is not null
group by platform, churn




-- COMMAND ----------

-- 6. Predicted to churn by channel
-- Create a pie chart visualisation

SELECT channel, count(*) as users
FROM churn_prediction
WHERE churn_prediction=1 and channel is not null
GROUP BY channel


-- COMMAND ----------

-- 7. Predicted to churn by country
-- Create a bar visualisation

SELECT country, churn_prediction, count(*) as customers
FROM churn_prediction
GROUP BY country, churn_prediction
