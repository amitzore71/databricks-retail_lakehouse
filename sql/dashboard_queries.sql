-- Daily revenue trend and AOV by channel
SELECT
  to_date(order_ts) AS dt,
  channel,
  ROUND(SUM(quantity * price), 2) AS gross_revenue,
  ROUND(
    SUM(
      CASE
        WHEN lower(status) IN ('canceled', 'cancelled', 'returned', 'return_requested', 'unavailable') THEN 0
        ELSE quantity * price
      END
    ),
    2
  ) AS net_revenue,
  COUNT(DISTINCT order_id) AS order_count,
  ROUND(
    SUM(
      CASE
        WHEN lower(status) IN ('canceled', 'cancelled', 'returned', 'return_requested', 'unavailable') THEN 0
        ELSE quantity * price
      END
    ) / NULLIF(COUNT(DISTINCT order_id), 0),
    2
  ) AS aov
FROM main.retail_p1.silver_orders_clean
GROUP BY to_date(order_ts), channel
ORDER BY dt, channel;

-- Customer lifetime value segments
SELECT
  customer_id,
  ltv_90d,
  ltv_180d,
  ltv_total,
  CASE
    WHEN ltv_total >= 1000 THEN 'VIP'
    WHEN ltv_total >= 400 THEN 'High'
    WHEN ltv_total >= 150 THEN 'Mid'
    ELSE 'Low'
  END AS ltv_segment
FROM main.retail_p1.gold_customer_ltv
ORDER BY ltv_total DESC;

-- Product return and cancellation rates by category and date
SELECT
  o.dt,
  COALESCE(p.category, 'unknown') AS category,
  COUNT(DISTINCT o.order_id) AS order_count,
  COUNT(DISTINCT CASE WHEN lower(o.status) IN ('returned', 'return_requested') THEN o.order_id END) AS returned_orders,
  COUNT(DISTINCT CASE WHEN lower(o.status) IN ('canceled', 'cancelled', 'unavailable') THEN o.order_id END) AS canceled_orders,
  ROUND(
    COUNT(DISTINCT CASE WHEN lower(o.status) IN ('returned', 'return_requested') THEN o.order_id END)
    / NULLIF(COUNT(DISTINCT o.order_id), 0),
    4
  ) AS return_rate,
  ROUND(
    COUNT(DISTINCT CASE WHEN lower(o.status) IN ('canceled', 'cancelled', 'unavailable') THEN o.order_id END)
    / NULLIF(COUNT(DISTINCT o.order_id), 0),
    4
  ) AS cancellation_rate
FROM (
  SELECT
    order_id,
    product_id,
    lower(status) AS status,
    to_date(order_ts) AS dt
  FROM main.retail_p1.silver_orders_clean
) o
LEFT JOIN main.retail_p1.silver_products_latest p
  ON o.product_id = p.product_id
GROUP BY o.dt, COALESCE(p.category, 'unknown')
ORDER BY o.dt, category;
