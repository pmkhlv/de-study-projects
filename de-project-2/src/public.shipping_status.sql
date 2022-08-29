-- DROP TABLE shipping_status;

CREATE TABLE shipping_status (
	shippingid BIGINT, 
	status TEXT,
	state TEXT,
	shipping_start_fact_datetime TIMESTAMP, -- это время state_datetime, когда state заказа перешёл в состояние booked
	shipping_end_fact_datetime TIMESTAMP -- это время state_datetime , когда state заказа перешёл в состояние received
	);


INSERT INTO shipping_status (shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
SELECT shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime
FROM (
	WITH
	state_booked_cte AS (
		SELECT 
			shippingid,
			state_datetime AS shipping_start_fact_datetime
		FROM 
			shipping s 
		WHERE state = 'booked'
		),
		
	state_received_cte AS (
		SELECT 
			shippingid,
			state_datetime AS shipping_end_fact_datetime
		FROM 
			shipping s 
		WHERE state = 'recieved')

	SELECT shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime
	FROM (SELECT DISTINCT 
			shippingid, 
			LAST_VALUE(status) OVER w AS status,
			LAST_VALUE(state) OVER w AS state
			FROM shipping
			WINDOW w AS (PARTITION BY shipping.shippingid 
						 ORDER BY shipping.state_datetime 
						 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) last_status_state
	INNER JOIN state_booked_cte USING (shippingid)
	-- Было: INNER JOIN state_received_cte USING (shippingid)) start_end_datetime;
	LEFT JOIN state_received_cte USING (shippingid)) start_end_datetime;

-- SELECT * FROM shipping_status;
