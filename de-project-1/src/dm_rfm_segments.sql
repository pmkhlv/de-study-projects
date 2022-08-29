CREATE OR REPLACE VIEW analysis.orderitems_v AS (
  SELECT * FROM production.orderitems);
 
CREATE OR REPLACE VIEW analysis.orders_v AS (
  SELECT * FROM production.orders);
  
CREATE OR REPLACE VIEW analysis.orderstatuses_v AS (
  SELECT * FROM production.orderstatuses);
  
CREATE OR REPLACE VIEW analysis.products_v AS (
  SELECT * FROM production.products);
  
CREATE OR REPLACE VIEW analysis.users_v AS (
  SELECT * FROM production.users);
  
CREATE TABLE IF NOT EXISTS analysis.dm_rfm_segments (
    user_id INTEGER,
    recency SMALLINT NOT NULL CHECK (recency BETWEEN 1 AND 5),
    frequency SMALLINT NOT NULL CHECK (frequency BETWEEN 1 AND 5),
    monetary_value SMALLINT NOT NULL CHECK (monetary_value BETWEEN 1 AND 5),
    PRIMARY KEY (user_id),
    FOREIGN KEY (user_id) REFERENCES production.users("id"));
 
 
INSERT INTO analysis.dm_rfm_segments (
    user_id,
    recency,
    frequency,
    monetary_value)
 
SELECT
    id,
    NTILE(5) OVER (ORDER BY last_order_date asc) AS recency_group,
    NTILE(5) OVER (ORDER BY sum_orders asc) AS frequency_group,
    NTILE(5) OVER (ORDER BY sum_payment) AS monetary_group
FROM (
  SELECT 
    id, last_order_date, sum_orders, sum_payment
  FROM 
    analysis.users_v u 
  LEFT JOIN (SELECT
            DISTINCT(user_id),
            MAX(order_ts) OVER (PARTITION BY user_id) AS last_order_date, -- дата последнего заказа
            COUNT(*) OVER (PARTITION BY user_id) AS sum_orders, -- количество заказов у каждого клиента
            SUM(payment) OVER (PARTITION BY user_id) AS sum_payment -- сумма заказов по клиенту
            FROM analysis.orders_v 
            WHERE order_ts > '2021-01-01' AND status = 4) o 
  ON u.id = o.user_id) users_orders;
  
SELECT COUNT(*) FROM analysis.dm_rfm_segments;
 
 
-- Доработка представлений
CREATE OR REPLACE VIEW analysis.orders_v
AS SELECT orders.order_id,
    orders.order_ts,
    orders.user_id,
    orders.bonus_payment,
    orders.payment,
    orders.COST,
    orders.bonus_grant,
    orders.status
FROM production.orders LEFT JOIN (
     SELECT 
        orderstatuslog_v.order_id,
        LAST_VALUE(orderstatuslog_v.status_id) OVER (PARTITION BY orderstatuslog_v.order_id
                                                     ORDER BY orderstatuslog_v.dttm ASC
                                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS status
      FROM
        analysis.orderstatuslog_v) status_query ON production.orders.order_id = status_query.order_id;
