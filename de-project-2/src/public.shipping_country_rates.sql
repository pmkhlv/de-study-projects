-- DROP TABLE public.shipping_country_rates;

CREATE TABLE shipping_country_rates (
	shipping_country_id SERIAL,
	shipping_country VARCHAR(30) UNIQUE,
	shipping_country_base_rate NUMERIC (14, 3),
	PRIMARY KEY ("shipping_country_id")
);

INSERT INTO shipping_country_rates (shipping_country, shipping_country_base_rate)
SELECT DISTINCT shipping_country, shipping_country_base_rate
FROM public.shipping 
ORDER BY shipping_country ASC;

-- SELECT * FROM shipping_country_rates LIMIT 10;
