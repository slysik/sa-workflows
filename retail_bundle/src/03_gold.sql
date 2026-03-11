-- ============================================================================
-- GOLD LAYER — Business-ready aggregations for BI/serving.
-- Pre-aggregated, stable column contracts, consumption-shaped.
--
-- SA NARRATION:
--   "Gold tables are the API contract between data engineering and business consumers.
--    Pre-aggregated means dashboards query Gold directly — no ad-hoc GROUP BYs on Silver.
--    Stable column names mean BI tools don't break when upstream logic changes.
--    Each table is shaped for a specific consumption pattern: daily KPIs, product mix,
--    customer LTV, regional rollup. This is the Kimball star schema on Delta Lake."
--
-- LIQUID CLUSTERING — WHY IT BEATS PARTITION BY:
--   1. No small-file problem — auto-compacts. PARTITION BY (order_date) → 180 tiny dirs.
--   2. Multi-column data skipping via Hilbert space-filling curves.
--   3. Keys are MUTABLE: ALTER TABLE ... CLUSTER BY (new_col). Zero downtime.
--   4. No cardinality limit — works on any column, not just low-cardinality partitions.
--   5. 97%+ data skipping in production scan benchmarks.
--
-- VERIFY AFTER PIPELINE:
--   DESCRIBE DETAIL interview.retail.gold_daily_sales;   -- clusteringColumns
--   EXPLAIN SELECT * FROM interview.retail.gold_regional_summary
--     WHERE region = 'West' AND order_date >= '2025-10-01';  -- PushedFilters
-- ============================================================================


-- ┌──────────────────────────────────────────────────────────────────┐
-- │ Gold: Daily Sales Summary                                       │
-- │                                                                  │
-- │ SA: Primary KPI table for the executive dashboard.               │
-- │     Pre-joins orders × order_items — dashboard queries Gold,     │
-- │     not Silver. No ad-hoc joins at query time.                   │
-- │     CLUSTER BY (order_date, channel) → Hilbert curve maps both   │
-- │     columns into a single sort. "Show me web orders last 30d"    │
-- │     skips 90%+ files. Unlike PARTITION BY (order_date), no risk  │
-- │     of 180 tiny directories.                                     │
-- └──────────────────────────────────────────────────────────────────┘
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
CLUSTER BY (order_date, channel)
COMMENT 'Daily sales KPIs by date and channel — Liquid Clustering for multi-column skipping'
AS
SELECT
  o.order_date,
  o.channel,
  COUNT(DISTINCT o.order_id)                                           AS order_count,
  COUNT(DISTINCT o.customer_id)                                        AS unique_customers,
  SUM(oi.line_total)                                                   AS total_revenue,
  ROUND(SUM(oi.line_total) / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS avg_order_value,
  SUM(oi.quantity)                                                     AS total_units_sold
FROM silver_orders o
INNER JOIN silver_order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_date, o.channel;


-- ┌──────────────────────────────────────────────────────────────────┐
-- │ Gold: Product Performance                                        │
-- │                                                                  │
-- │ SA: Enables "top products by revenue" and margin analysis.       │
-- │     Gross margin = revenue - (quantity × cost_price). Margin %   │
-- │     enables apples-to-apples comparison across price tiers.      │
-- │     CLUSTER BY (category, product_id) → category rollup queries  │
-- │     skip all non-target categories. Drill-down to product_id     │
-- │     within a category scans minimal files via Hilbert co-location│
-- └──────────────────────────────────────────────────────────────────┘
CREATE OR REFRESH MATERIALIZED VIEW gold_product_performance
CLUSTER BY (category, product_id)
COMMENT 'Product revenue and margin — Liquid Clustering on (category, product_id)'
AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.unit_price                                                         AS list_price,
  p.cost_price,
  COUNT(DISTINCT oi.order_id)                                          AS order_count,
  SUM(oi.quantity)                                                     AS total_units_sold,
  SUM(oi.line_total)                                                   AS total_revenue,
  -- SA: Gross margin = revenue minus COGS. Key profitability metric.
  ROUND(SUM(oi.line_total) - SUM(oi.quantity * p.cost_price), 2)       AS gross_margin,
  -- SA: Margin % enables cross-category comparison. Electronics high-revenue but low-margin.
  ROUND(
    (SUM(oi.line_total) - SUM(oi.quantity * p.cost_price))
    / NULLIF(SUM(oi.line_total), 0) * 100, 1
  )                                                                    AS margin_pct
FROM silver_order_items oi
INNER JOIN silver_products p ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category, p.unit_price, p.cost_price;


-- ┌──────────────────────────────────────────────────────────────────┐
-- │ Gold: Customer Lifetime Value                                    │
-- │                                                                  │
-- │ SA: Customer-grain aggregation for segmentation and LTV analysis.│
-- │     LEFT JOIN preserves customers with zero orders (signup but    │
-- │     never purchased). days_since_last_order = recency metric.    │
-- │     CLUSTER BY (region, loyalty_tier) → "Show me VIP customers   │
-- │     in West region" achieves 97% data skipping. To pivot to      │
-- │     recency-based analysis: ALTER TABLE ... CLUSTER BY           │
-- │     (customer_id, loyalty_tier) — instant, no rewrite.           │
-- └──────────────────────────────────────────────────────────────────┘
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_ltv
CLUSTER BY (region, loyalty_tier)
COMMENT 'Customer LTV — Liquid Clustering on (region, loyalty_tier) for segmentation queries'
AS
SELECT
  c.customer_id,
  c.first_name,
  c.region,
  c.loyalty_tier,
  c.signup_date,
  COUNT(DISTINCT o.order_id)                                           AS total_orders,
  SUM(oi.line_total)                                                   AS total_spend,
  ROUND(SUM(oi.line_total) / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS avg_order_value,
  MIN(o.order_date)                                                    AS first_order_date,
  MAX(o.order_date)                                                    AS last_order_date,
  -- SA: Recency metric — how many days since last purchase. Key for churn prediction.
  DATEDIFF(CURRENT_DATE(), MAX(o.order_date))                          AS days_since_last_order
FROM silver_customers c
LEFT JOIN silver_orders o ON c.customer_id = o.customer_id
LEFT JOIN silver_order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.first_name, c.region, c.loyalty_tier, c.signup_date;


-- ┌──────────────────────────────────────────────────────────────────┐
-- │ Gold: Regional Summary                                           │
-- │                                                                  │
-- │ SA: Executive dashboard pattern: "Show me West region, last 30d" │
-- │     CLUSTER BY (region, order_date) → Hilbert curve co-locates   │
-- │     (region, date) pairs. WHERE region = 'West' AND order_date   │
-- │     >= '2025-10-01' scans only files containing West + recent.   │
-- │     This is the canonical LC vs PARTITION BY example:             │
-- │       PARTITION BY (order_date) → 180 directories of tiny files  │
-- │       PARTITION BY (region) → only 4 dirs, no date skipping      │
-- │       CLUSTER BY (region, order_date) → ~10 files, both skip     │
-- └──────────────────────────────────────────────────────────────────┘
CREATE OR REFRESH MATERIALIZED VIEW gold_regional_summary
CLUSTER BY (region, order_date)
COMMENT 'Regional performance — Liquid Clustering on (region, order_date) for exec dashboard'
AS
SELECT
  c.region,
  o.order_date,
  COUNT(DISTINCT o.order_id)                                           AS order_count,
  COUNT(DISTINCT o.customer_id)                                        AS active_customers,
  SUM(oi.line_total)                                                   AS total_revenue,
  ROUND(AVG(oi.line_total), 2)                                        AS avg_item_value
FROM silver_orders o
INNER JOIN silver_customers c ON o.customer_id = c.customer_id
INNER JOIN silver_order_items oi ON o.order_id = oi.order_id
GROUP BY c.region, o.order_date;
