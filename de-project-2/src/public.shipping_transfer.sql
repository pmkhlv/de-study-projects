-- DROP TABLE shipping_transfer;

CREATE TABLE shipping_transfer (
	transfer_type_id SERIAL,
	transfer_type VARCHAR(3) NOT NULL,
	transfer_model VARCHAR(15) NOT NULL,
	shipping_transfer_rate NUMERIC(3,3) NOT NULL,
	PRIMARY KEY (transfer_type_id));
	
INSERT INTO shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
SELECT DISTINCT 
	(pg_catalog.regexp_split_to_array(shipping_transfer_description, E'\\:'))[1] AS transfer_type,
	(pg_catalog.regexp_split_to_array(shipping_transfer_description, E'\\:'))[2] AS transfer_model,
	shipping_transfer_rate
FROM 
	public.shipping;
	
-- SELECT * FROM shipping_transfer LIMIT 10;