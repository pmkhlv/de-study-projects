INSERT INTO cdm.dm_courier_ledger (
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_reward_sum
	)
SELECT *,
	(ag2.courier_order_sum + ag2.courier_tips_sum * 0.95) AS courier_reward_sum
FROM (
	SELECT courier_id,
		courier_name,
		settlement_year,
		settlement_month,
		sum(orders_count) AS orders_count,
		sum(orders_total_sum) AS orders_total_sum,
		sum(rate_avg) AS rate_avg,
		(sum(ag.orders_total_sum) * 0.25)::NUMERIC(14, 2) AS order_processing_fee,
		sum(CASE 
				WHEN ag.rate_avg < 4
					THEN GREATEST(0.05 * ag.orders_total_sum, 100)::NUMERIC(14, 2)
				WHEN ag.rate_avg >= 4 AND ag.rate_avg < 4.5
					THEN GREATEST(0.07 * ag.orders_total_sum, 150)::NUMERIC(14, 2)
				WHEN ag.rate_avg >= 4.5 AND ag.rate_avg < 4.9
					THEN GREATEST(0.08 * ag.orders_total_sum, 175)::NUMERIC(14, 2)
				WHEN ag.rate_avg >= 4.9
					THEN GREATEST(0.10 * ag.orders_total_sum, 200)::NUMERIC(14, 2)
				END) AS courier_order_sum,
		sum(courier_tips_sum) AS courier_tips_sum
	FROM (
		SELECT fd.courier_id,
			c.courier_name,
			ots.order_year AS settlement_year,
			ots.order_month AS settlement_month,
			count(fd.order_id)::NUMERIC(14, 2) AS orders_count,
			sum(fd.order_sum)::NUMERIC(14, 2) AS orders_total_sum,
			avg(fd.rate)::NUMERIC(3, 2) AS rate_avg,
			sum(fd.tip_sum)::NUMERIC(14, 2) AS courier_tips_sum
		FROM dds.fct_delivery fd
		INNER JOIN dds.order_ts ots ON fd.order_ts_id = ots.id
		INNER JOIN dds.couriers c ON fd.courier_id = c.id
		GROUP BY 1, 2, 3, 4
		) AS ag
	GROUP BY 1, 2, 3, 4
	) AS ag2;