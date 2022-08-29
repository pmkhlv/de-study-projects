-- DROP TABLE shipping_agreement;

CREATE TABLE shipping_agreement (
	agreementid BIGINT NOT NULL ,
	agreement_number VARCHAR(15) NOT NULL,
	agreement_rate NUMERIC(3, 3) NOT NULL,
	agreement_commission NUMERIC(3, 3) NOT NULL,
	UNIQUE (agreementid, agreement_number),
	PRIMARY KEY (agreementid)
);

INSERT INTO shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT DISTINCT
	(pg_catalog.regexp_split_to_array(vendor_agreement_description, E'\\:'))[1]::BIGINT AS agreementid,
	(pg_catalog.regexp_split_to_array(vendor_agreement_description, E'\\:'))[2]::VARCHAR(15) AS agreement_number,
	(pg_catalog.regexp_split_to_array(vendor_agreement_description, E'\\:'))[3]::NUMERIC(3, 3) AS agreement_rate,
	(pg_catalog.regexp_split_to_array(vendor_agreement_description, E'\\:'))[4]::NUMERIC(3, 3) AS agreement_commission
FROM shipping;

-- SELECT * FROM shipping_agreement LIMIT 10;
