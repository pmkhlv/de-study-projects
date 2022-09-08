-- dds.couriers definition

CREATE TABLE dds.couriers (
	id serial4 NOT NULL,
	courier_id varchar(255) NOT NULL,
	courier_name text NOT NULL,
	CONSTRAINT couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT couriers_pkey PRIMARY KEY (id),
	CONSTRAINT couriers_un UNIQUE (courier_id)
);


-- dds.delivery_ts definition

CREATE TABLE dds.delivery_ts (
	id serial4 NOT NULL,
	delivery_ts timestamp NOT NULL,
	delivery_year int4 NULL,
	delivery_month int4 NULL,
	delivery_day int4 NULL,
	CONSTRAINT dds_delivery_ts_check_delivery_day CHECK (((delivery_day >= 1) AND (delivery_day <= 31))),
	CONSTRAINT dds_delivery_ts_check_delivery_month CHECK (((delivery_month >= 1) AND (delivery_month <= 12))),
	CONSTRAINT dds_delivery_ts_check_delivery_year CHECK (((delivery_year >= 2000) AND (delivery_year <= 2200))),
	CONSTRAINT delivery_ts_pkey PRIMARY KEY (id),
	CONSTRAINT delivery_ts_un UNIQUE (delivery_ts)
);


-- dds.deliveries definition

CREATE TABLE dds.deliveries (
	id serial4 NOT NULL,
	delivery_id varchar(255) NOT NULL,
	courier_id int4 NOT NULL,
	delivery_address varchar(255) NOT NULL,
	delivery_ts timestamp NULL,
	rate numeric(14, 2) NULL,
	tip_sum numeric(14, 2) NULL,
	delivery_ts_id int4 NULL,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT deliveries_rate_check CHECK ((rate >= (0)::numeric)),
	CONSTRAINT deliveries_tip_sum_check CHECK ((tip_sum >= (0)::numeric)),
	CONSTRAINT deliveries_un UNIQUE (delivery_id),
	CONSTRAINT foreign_key_dds_deliveries_dds_couriers FOREIGN KEY (courier_id) REFERENCES dds.couriers(id),
	CONSTRAINT foreign_key_dds_deliveries_dds_delivery_ts FOREIGN KEY (delivery_ts_id) REFERENCES dds.delivery_ts(id)
);


-- dds.order_ts definition

CREATE TABLE dds.order_ts (
	id serial4 NOT NULL,
	order_ts timestamp NOT NULL,
	order_year int4 NULL,
	order_month int4 NULL,
	order_day int4 NULL,
	CONSTRAINT dds_order_ts_check_order_day CHECK (((order_day >= 1) AND (order_day <= 31))),
	CONSTRAINT dds_order_ts_check_order_month CHECK (((order_month >= 1) AND (order_month <= 12))),
	CONSTRAINT dds_order_ts_check_order_year CHECK (((order_year >= 2000) AND (order_year <= 2200))),
	CONSTRAINT order_ts_pkey PRIMARY KEY (id),
	CONSTRAINT order_ts_un UNIQUE (order_ts)
);


-- dds.orders definition

CREATE TABLE dds.orders (
	id serial4 NOT NULL,
	order_id varchar(255) NOT NULL,
	order_ts timestamp NOT NULL,
	order_sum numeric(14, 2) NOT NULL,
	order_ts_id int4 NULL,
	CONSTRAINT orders_order_sum_check CHECK ((order_sum >= (0)::numeric)),
	CONSTRAINT orders_pkey PRIMARY KEY (id),
	CONSTRAINT orders_un UNIQUE (order_id),
	CONSTRAINT foreign_key_dds_orders_dds_orders_ts FOREIGN KEY (order_ts_id) REFERENCES dds.order_ts(id)
);


-- dds.fct_delivery definition

CREATE TABLE dds.fct_delivery (
	id serial4 NOT NULL,
	order_id int8 NOT NULL,
	order_ts_id int4 NOT NULL,
	delivery_id int8 NOT NULL,
	delivery_ts_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	rate numeric(14, 2) NOT NULL,
	order_sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT fct_delivery_order_sum_check CHECK ((order_sum >= (0)::numeric)),
	CONSTRAINT fct_delivery_pkey PRIMARY KEY (id),
	CONSTRAINT fct_delivery_rate_check CHECK ((order_sum >= (0)::numeric)),
	CONSTRAINT fct_delivery_tip_sum_check CHECK ((order_sum >= (0)::numeric)),
	CONSTRAINT fct_delivery_un UNIQUE (order_id, delivery_id),
	CONSTRAINT foreign_key_fct_delivery_deliveries FOREIGN KEY (delivery_id) REFERENCES dds.deliveries(id),
	CONSTRAINT foreign_key_fct_delivery_orders FOREIGN KEY (order_id) REFERENCES dds.orders(id)
);