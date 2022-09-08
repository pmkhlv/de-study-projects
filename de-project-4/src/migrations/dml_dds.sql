-- dds.ORDERS_TS

INSERT INTO dds.order_ts
(order_ts, order_year, order_month, order_day)
SELECT 
	order_ts,
	date_part('year', order_ts)::integer,
	date_part('month', order_ts)::integer,
	date_part('day', order_ts)::integer
FROM stg.deliveries
ON CONFLICT (order_ts) DO NOTHING;


-- dds.DELIVERIES_TS

INSERT INTO dds.delivery_ts
(delivery_ts, delivery_year, delivery_month, delivery_day)
SELECT 
	delivery_ts,
	date_part('year', delivery_ts)::integer,
	date_part('month', delivery_ts)::integer,
	date_part('day', delivery_ts)::integer
FROM stg.deliveries
ON CONFLICT (delivery_ts) DO NOTHING;


-- dds.COURIERS

INSERT INTO dds.couriers (courier_id, courier_name)
SELECT courier_id, courier_name 
FROM stg.couriers
ON CONFLICT (courier_id) DO UPDATE 
SET
courier_name = excluded.courier_name;


-- dds.DELIVERIES

INSERT INTO dds.deliveries
(delivery_id, courier_id, delivery_address, delivery_ts, rate, tip_sum, delivery_ts_id)
SELECT d.delivery_id, c.id, d.address, d.delivery_ts, d.rate, d.tip_sum, dts.id 
FROM stg.deliveries d 
INNER JOIN dds.couriers c ON d.courier_id = c.courier_id
INNER JOIN dds.delivery_ts dts ON d.delivery_ts = dts.delivery_ts 
ON CONFLICT (delivery_id) DO UPDATE 
SET
delivery_ts = excluded.delivery_ts,
rate = excluded.rate,
tip_sum = excluded.tip_sum;


-- dds.ORDERS

INSERT INTO dds.orders 
(order_id, order_ts, order_sum, order_ts_id)
SELECT d.order_id, d.order_ts, d.order_sum, ots.id 
FROM stg.deliveries d
INNER JOIN dds.order_ts ots ON d.order_ts = ots.order_ts 
ON CONFLICT (order_id) DO 
UPDATE SET
order_ts = excluded.order_ts,
order_sum = excluded.order_sum;


--- FCT_DELIVERY

INSERT INTO dds.fct_delivery
(order_id, order_ts_id, delivery_id, delivery_ts_id, courier_id, rate, order_sum, tip_sum)
SELECT 
		o.id AS order_id, 
		ots.id AS order_ts_id, 
		d.id AS delivery_id, 
		dts.id AS delivery_ts_id, 
		d.courier_id, 
		d.rate, 
		o.order_sum, 
		d.tip_sum
FROM stg.deliveries sd 
INNER JOIN dds.deliveries d ON sd.delivery_id = d.delivery_id 
INNER JOIN dds.orders o ON sd.order_id = o.order_id
INNER JOIN dds.delivery_ts dts ON sd.delivery_ts = dts.delivery_ts 
INNER JOIN dds.order_ts ots ON sd.order_ts = ots.order_ts 
ON CONFLICT (order_id, delivery_id) DO 
UPDATE SET
order_ts_id = excluded.order_ts_id,
delivery_ts_id = excluded.delivery_ts_id,
courier_id = excluded.courier_id,
rate = excluded.rate,
order_sum = excluded.order_sum,
tip_sum = excluded.tip_sum;