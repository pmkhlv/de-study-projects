-- stg.couriers definition

CREATE TABLE stg.couriers (
	courier_id varchar(255) NOT NULL,
	courier_name varchar(255) NOT NULL,
	CONSTRAINT couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT couriers_pk PRIMARY KEY (courier_id),
	CONSTRAINT couriers_un UNIQUE (courier_id)
);


-- stg.deliveries definition

CREATE TABLE stg.deliveries (
	order_id varchar(255) NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar(255) NOT NULL,
	courier_id varchar(255) NOT NULL,
	address varchar(255) NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate numeric(3, 2) NOT NULL,
	order_sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT deliveries_pk PRIMARY KEY (order_id, delivery_id),
	CONSTRAINT deliveries_un UNIQUE (order_id, delivery_id),
	CONSTRAINT stg_deliveries_order_sum_check CHECK ((order_sum >= (0)::numeric)),
	CONSTRAINT stg_deliveries_rate_check CHECK ((rate >= (0)::numeric)),
	CONSTRAINT stg_deliveries_tip_sum_check CHECK ((tip_sum >= (0)::numeric))
);


-- stg.restaurants definition

CREATE TABLE stg.restaurants (
	restaurant_id varchar(255) NOT NULL,
	restaurant_name varchar(255) NOT NULL,
	CONSTRAINT restaurants_pk PRIMARY KEY (restaurant_id),
	CONSTRAINT restaurants_restaurant_id_key UNIQUE (restaurant_id),
	CONSTRAINT restaurants_un UNIQUE (restaurant_id)
);

