CREATE OR REPLACE VIEW shipping_datamart AS (
	SELECT DISTINCT
		si.shippingid,
		si.vendorid,
		st.transfer_type,
		DATE_PART('day', AGE(ss.shipping_end_fact_datetime, ss.shipping_start_fact_datetime)) AS full_day_at_shipping,
		CASE WHEN ss.shipping_end_fact_datetime > s.shipping_plan_datetime THEN 1 ELSE 0 END  AS is_delay,
		CASE WHEN ss.status = 'finished' THEN 1 ELSE 0 END AS is_shipping_finish,
		CASE WHEN ss.shipping_end_fact_datetime > s.shipping_plan_datetime THEN DATE_PART('day', AGE(ss.shipping_end_fact_datetime, s.shipping_plan_datetime)) ELSE 0 END AS delay_day_at_shipping,
		si.payment_amount AS payment_amount,
		(si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate))::NUMERIC(14, 2) AS vat,
		(si.payment_amount * sa.agreement_commission)::NUMERIC(14, 2) AS profit
		
	FROM 
		shipping s 
		INNER JOIN shipping_info si USING (shippingid)
		INNER JOIN shipping_status ss USING (shippingid)
		INNER JOIN shipping_transfer st ON si.transfer_type_id = st.transfer_type_id
		INNER JOIN shipping_country_rates scr ON si.shipping_country_id = scr.shipping_country_id 
		INNER JOIN shipping_agreement sa ON si.agreementid = sa.agreementid
);

-- SELECT * FROM shipping_datamart LIMIT 50;