WITH HT_1 AS 
-- This query references multiple calculated fields that were created in an inner query
(
SELECT  subscription_id,
    payment_gateway,
    product_group,
    product_sub_group,
    product_slug,
    period_months,
    started_at,
    ended_at,
    is_auto_renew,
    ar_valid_from,
    billings_eur_excl_vat,
    length_of_subscription_days,
    period_days,
    unusual_ar_dates,
    days_ar_enabled,
    days_ar_enabled / period_days AS 'ar_elapsed_%', -- This calculates the % of subscription that AR was enabled
    CASE 
    WHEN days_ar_enabled / period_days <1 
    THEN 'TRUE' ELSE 'FALSE' 
    END AS ar_was_disabled, -- This expression calculates whether AR was disabled before subscription finished 
    CASE 
    WHEN days_ar_enabled / period_days <1 
        THEN MONTH(ar_valid_to) 
    END AS month_ar_disabled -- This expression identifies on which month AR was disabled
    FROM 
    
(-- The following query creates multiple calculated fields which are used above
SELECT 
    subscription_id,
    payment_gateway,
    product_group,
    product_sub_group,
    product_slug,
    period_months,
    started_at,
    ended_at,
    is_auto_renew,
    ar_valid_from,
    ar_valid_to,
    billings_eur_excl_vat,
    DATEDIFF(ended_at,started_at) AS length_of_subscription_days, -- Length of subscription in days
    /* The following expression calculates the #f days for subscriptions that have a 1 month period. 
    This is needed as the subscription ends the following month on the same day of month that it started, regardless of the # of days between */
    CASE 
    WHEN period_months = 12 
        THEN 365
    WHEN period_months = 1 
        THEN DATEDIFF(DATE(CONCAT(YEAR(DATE_ADD(started_at, INTERVAL 1 MONTH)), '-', MONTH(DATE_ADD(started_at, INTERVAL 1 MONTH)), '-', DAYOFMONTH(started_at))), (started_at))
    END AS period_days,
    /* The following expression identifies AR dates that appear incorrect */
    CASE
    WHEN ar_valid_to < ar_valid_from 
        THEN 'TRUE' 
    WHEN ar_valid_from < started_at 
        THEN 'TRUE'
        ELSE 'FALSE' 
    END AS unusual_ar_dates, 
    /* The following expression calculates the # of months AR was enabled */
    PERIOD_DIFF(
    CAST(CONCAT(YEAR(ar_valid_to),
    CASE WHEN MONTH(ar_valid_to) <10 
        THEN CONCAT('0',MONTH(ar_valid_to)) ELSE MONTH(ar_valid_to) 
    END) AS LONG)
    ,
    CAST(CONCAT(YEAR(ar_valid_from),
    CASE WHEN MONTH(ar_valid_from) <10 
    THEN CONCAT('0',MONTH(ar_valid_from)) ELSE MONTH(ar_valid_from) 
    END) AS LONG)) AS months_ar_enabled,
    DATEDIFF(ar_valid_to,ar_valid_from) AS days_ar_enabled -- This expression calculates the # of days are was enabled
FROM `bquxjob_2795cbba_191519f7494.csv`
) AS x)
,
/* The following query groups by subscription id to identify subscriptions with more than 1 record */
dupes AS (
    SELECT 
    `subscription_id` AS sub_id,
    COUNT(subscription_id) AS count_of_records_per_sub
    FROM `bquxjob_2795cbba_191519f7494.csv`
    GROUP BY 1)    
/*The following query joins the dupes table onto the main table HT_1 
to allow for filtering on the created count_of_records_per_sub field */
SELECT *
FROM HT_1
LEFT JOIN dupes
    ON HT_1.subscription_id = dupes.sub_id 
