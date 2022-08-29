-- DROP TABLE shipping_info;

CREATE TABLE shipping_info (
	"id" SERIAL,
	shippingid BIGINT NOT NULL,
	vendorid BIGINT NOT NULL,
	payment_amount NUMERIC(14,2) NOT NULL,
	shipping_plan_datetime TIMESTAMP NOT NULL,
	transfer_type_id BIGINT NOT NULL,
	shipping_country_id BIGINT NOT NULL,
	agreementid SMALLINT NOT NULL,
	PRIMARY KEY ("id"),
	FOREIGN KEY (transfer_type_id) REFERENCES shipping_transfer(transfer_type_id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_country_id) REFERENCES shipping_country_rates(shipping_country_id) ON UPDATE CASCADE,
	FOREIGN KEY (agreementid) REFERENCES shipping_agreement(agreementid) ON UPDATE CASCADE
);

INSERT INTO shipping_info (
		shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
SELECT DISTINCT shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid
FROM 
	public.shipping s 
INNER JOIN 
	shipping_transfer st ON
		(regexp_split_to_array(s.shipping_transfer_description, E'\\:'))[1] = st.transfer_type
		AND
		(regexp_split_to_array(s.shipping_transfer_description, E'\\:'))[2] = st.transfer_model
INNER JOIN 
	shipping_country_rates scr ON
		s.shipping_country = scr.shipping_country
INNER JOIN 
	shipping_agreement sa ON
		(regexp_split_to_array(s.vendor_agreement_description, E'\\:'))[1]::BIGINT = sa.agreementid;
		
-- SELECT * FROM shipping_info LIMIT 50;
