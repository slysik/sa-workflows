-- ============================================================================
-- SILVER LAYER — Cleaned, typed, deduped, quality-enforced.
-- Reusable business semantics. Explicit column contracts.
--
-- SA NARRATION:
--   "Silver is where data quality becomes a first-class citizen.
--    Every column is explicitly CAST — no schema inference.
--    ROW_NUMBER dedup is deterministic and idempotent — safe for re-runs.
--    Expectations with ON VIOLATION DROP ROW act as quality gates —
--    bad rows are quarantined, not silently passed downstream.
--    This is the reusable semantic layer that Gold, BI, and ML all consume."
--
-- WHY MATERIALIZED VIEWS (not streaming tables):
--   Bronze tables were written by spark.range() → saveAsTable() — they're
--   regular managed Delta tables, NOT streaming tables. SDP materialized
--   views are the correct abstraction: full recompute on refresh, ACID,
--   and expectations support. If Bronze were Auto Loader streaming tables,
--   we'd use CREATE OR REFRESH STREAMING TABLE here instead.
--
-- LIQUID CLUSTERING STRATEGY (Silver):
--   Keys match downstream Gold join/filter patterns:
--     customers:    (region, loyalty_tier) → segmentation queries
--     products:     (category)            → category rollups
--     orders:       (order_date, channel) → time-series + channel analysis
--     order_items:  (order_id)            → co-locate with orders for joins
--   Keys are MUTABLE: ALTER TABLE ... CLUSTER BY (new_col) — instant change.
-- ============================================================================


-- SA: Customers — dedup on customer_id, COALESCE null emails, enforce NOT NULL on key columns.
--     ROW_NUMBER PARTITION BY customer_id: deterministic, idempotent, no data loss.
--     COALESCE(email, 'unknown@missing.com'): Silver normalizes nulls — downstream never sees them.
--     ON VIOLATION DROP ROW: rows failing expectations are quarantined, not silently propagated.
CREATE OR REFRESH MATERIALIZED VIEW silver_customers(
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_signup       EXPECT (signup_date IS NOT NULL) ON VIOLATION DROP ROW
)
CLUSTER BY (region, loyalty_tier)
COMMENT 'Cleaned customer dimension — deduped, nulls handled, schema enforced'
AS
SELECT
  CAST(customer_id AS STRING)    AS customer_id,
  CAST(first_name AS STRING)     AS first_name,
  COALESCE(email, 'unknown@missing.com') AS email,
  CAST(region AS STRING)         AS region,
  CAST(loyalty_tier AS STRING)   AS loyalty_tier,
  CAST(signup_date AS DATE)      AS signup_date
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS _rn
  FROM interview.retail.bronze_customers
)
WHERE _rn = 1;


-- SA: Products — enforce positive prices, dedup on product_id.
--     Two expectations: unit_price > 0 AND cost_price > 0.
--     If upstream sends a zero-priced product, it's dropped here — not in Gold aggregations.
--     CLUSTER BY (category): Gold product_performance groups by category, so Silver
--     pre-sorts data for that access pattern. Hilbert curve on single column = optimal.
CREATE OR REFRESH MATERIALIZED VIEW silver_products(
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price      EXPECT (unit_price > 0)         ON VIOLATION DROP ROW,
  CONSTRAINT valid_cost       EXPECT (cost_price > 0)         ON VIOLATION DROP ROW
)
CLUSTER BY (category)
COMMENT 'Cleaned product dimension — valid prices, deduped'
AS
SELECT
  CAST(product_id AS STRING)         AS product_id,
  CAST(product_name AS STRING)       AS product_name,
  CAST(category AS STRING)           AS category,
  CAST(unit_price AS DECIMAL(10,2))  AS unit_price,
  CAST(cost_price AS DECIMAL(10,2))  AS cost_price
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS _rn
  FROM interview.retail.bronze_products
)
WHERE _rn = 1;


-- SA: Orders — deterministic dedup removes the 1% duplicates injected in Bronze.
--     ORDER BY order_timestamp ASC: keeps the EARLIEST occurrence of each order_id.
--     Derived order_date from order_timestamp: enables date-grain Gold aggregations
--     without requiring Gold to re-derive. Silver is the reusable semantic layer.
--     CLUSTER BY (order_date, channel): matches Gold daily_sales GROUP BY pattern.
CREATE OR REFRESH MATERIALIZED VIEW silver_orders(
  CONSTRAINT valid_order_id    EXPECT (order_id IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL)     ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp   EXPECT (order_timestamp IS NOT NULL) ON VIOLATION DROP ROW
)
CLUSTER BY (order_date, channel)
COMMENT 'Cleaned order facts — deduped on order_id, typed, derived order_date'
AS
SELECT
  CAST(order_id AS STRING)            AS order_id,
  CAST(customer_id AS STRING)         AS customer_id,
  CAST(order_timestamp AS TIMESTAMP)  AS order_timestamp,
  CAST(order_timestamp AS DATE)       AS order_date,
  CAST(channel AS STRING)             AS channel,
  CAST(payment_method AS STRING)      AS payment_method,
  CAST(status AS STRING)              AS status
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_timestamp ASC) AS _rn
  FROM interview.retail.bronze_orders
)
WHERE _rn = 1;


-- SA: Order items — the outlier filter is the key quality gate here.
--     Bronze injected 1% line_totals at 50x normal and some negative values.
--     The expectation (line_total > 0 AND line_total < 50000) drops those rows.
--     This is declarative quality enforcement — no imperative code, no UDFs.
--     SDP tracks how many rows were dropped per expectation in pipeline metrics.
--     CLUSTER BY (order_id): co-locates items with their parent order for fast joins in Gold.
CREATE OR REFRESH MATERIALIZED VIEW silver_order_items(
  CONSTRAINT valid_item_id    EXPECT (item_id IS NOT NULL)                        ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_id   EXPECT (order_id IS NOT NULL)                       ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL)                     ON VIOLATION DROP ROW,
  CONSTRAINT positive_qty     EXPECT (quantity > 0)                               ON VIOLATION DROP ROW,
  CONSTRAINT valid_line_total EXPECT (line_total > 0 AND line_total < 50000)      ON VIOLATION DROP ROW
)
CLUSTER BY (order_id)
COMMENT 'Cleaned order line items — outliers filtered, typed, FK-safe'
AS
SELECT
  CAST(item_id AS STRING)              AS item_id,
  CAST(order_id AS STRING)             AS order_id,
  CAST(product_id AS STRING)           AS product_id,
  CAST(quantity AS INT)                AS quantity,
  CAST(unit_price AS DECIMAL(10,2))    AS unit_price,
  CAST(discount_pct AS DECIMAL(5,2))   AS discount_pct,
  CAST(line_total AS DECIMAL(12,2))    AS line_total
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY item_id) AS _rn
  FROM interview.retail.bronze_order_items
)
WHERE _rn = 1;
