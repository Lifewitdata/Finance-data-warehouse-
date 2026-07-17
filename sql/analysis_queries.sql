-- ============================================================================
-- Financial Transactions Data Warehouse — Analytical Queries
-- Run against the star schema built by etl/pipeline.py (data/finance_dw.db)
-- Demonstrates: joins across fact/dimension tables, CTEs, window functions
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. MONTHLY TRANSACTION VOLUME & VALUE (fact joined to dim_date)
-- ----------------------------------------------------------------------------
SELECT
    d.year,
    d.month_name,
    COUNT(*)                       AS txn_count,
    ROUND(SUM(f.amount), 2)        AS total_value,
    ROUND(AVG(f.amount), 2)        AS avg_txn_value
FROM fact_transactions f
JOIN dim_date d ON f.date_sk = d.date_sk
WHERE f.status = 'completed'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- ----------------------------------------------------------------------------
-- 2. SPEND BY MERCHANT CATEGORY (fact joined to dim_merchant)
-- ----------------------------------------------------------------------------
SELECT
    m.category,
    COUNT(*)                       AS txn_count,
    ROUND(SUM(f.amount), 2)        AS total_spend,
    ROUND(100.0 * SUM(f.amount) / (SELECT SUM(amount) FROM fact_transactions WHERE status='completed'), 2) AS pct_of_total
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
WHERE f.status = 'completed'
GROUP BY m.category
ORDER BY total_spend DESC;


-- ----------------------------------------------------------------------------
-- 3. CUSTOMER SEGMENT VALUE (fact -> dim_customer, CTE)
-- ----------------------------------------------------------------------------
WITH customer_spend AS (
    SELECT
        c.segment,
        c.customer_sk,
        SUM(f.amount) AS customer_total_spend
    FROM fact_transactions f
    JOIN dim_customer c ON f.customer_sk = c.customer_sk
    WHERE f.status = 'completed'
    GROUP BY c.segment, c.customer_sk
)
SELECT
    segment,
    COUNT(*)                                   AS customers,
    ROUND(AVG(customer_total_spend), 2)        AS avg_spend_per_customer,
    ROUND(SUM(customer_total_spend), 2)        AS segment_total_spend
FROM customer_spend
GROUP BY segment
ORDER BY segment_total_spend DESC;


-- ----------------------------------------------------------------------------
-- 4. TOP 5 CUSTOMERS BY SPEND PER CITY (window function: RANK, partitioned)
-- ----------------------------------------------------------------------------
WITH city_customer_spend AS (
    SELECT
        c.city,
        c.customer_sk,
        SUM(f.amount) AS total_spend,
        RANK() OVER (PARTITION BY c.city ORDER BY SUM(f.amount) DESC) AS spend_rank
    FROM fact_transactions f
    JOIN dim_customer c ON f.customer_sk = c.customer_sk
    WHERE f.status = 'completed' AND c.city != 'Unknown'
    GROUP BY c.city, c.customer_sk
)
SELECT city, customer_sk, ROUND(total_spend, 2) AS total_spend, spend_rank
FROM city_customer_spend
WHERE spend_rank <= 5
ORDER BY city, spend_rank;


-- ----------------------------------------------------------------------------
-- 5. TRANSACTION FAILURE RATE BY ACCOUNT TYPE (fact -> dim_account)
-- ----------------------------------------------------------------------------
SELECT
    a.account_type,
    COUNT(*)                                                          AS total_txns,
    SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END)              AS failed_txns,
    ROUND(100.0 * SUM(CASE WHEN f.status='failed' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
FROM fact_transactions f
JOIN dim_account a ON f.account_sk = a.account_sk
GROUP BY a.account_type
ORDER BY failure_rate_pct DESC;
