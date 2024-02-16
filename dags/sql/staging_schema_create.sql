create schema if not exists staging;

CREATE table if not exists staging.user_order_log (
	uniq_id varchar(32) NOT NULL,
	date_time timestamp NOT NULL,
	city_id int4 NOT NULL,
	city_name varchar(100) NULL,
	customer_id int4 NOT NULL,
	first_name varchar(100) NULL,
	last_name varchar(100) NULL,
	item_id int4 NOT NULL,
	item_name varchar(100) NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	status varchar(15),
	CONSTRAINT user_order_log_pk PRIMARY KEY (uniq_id)
);
CREATE INDEX if not exists uo1 ON staging.user_order_log USING btree (customer_id);
CREATE INDEX if not exists uo2 ON staging.user_order_log USING btree (item_id);
alter table if exists staging.user_order_log add column if not exists status varchar(15);

CREATE TABLE if not exists mart.f_sales (
	id serial4 NOT NULL,
	date_id int4 NOT NULL,
	item_id int4 NOT NULL,
	customer_id int4 NOT NULL,
	city_id int4 NOT NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	CONSTRAINT f_sales_pkey PRIMARY KEY (id),
	CONSTRAINT f_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id),
	CONSTRAINT f_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id),
	CONSTRAINT f_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id),
	CONSTRAINT f_sales_item_id_fkey1 FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
);
CREATE INDEX if not exists f_ds1 ON mart.f_sales USING btree (date_id);
CREATE INDEX if not exists f_ds2 ON mart.f_sales USING btree (item_id);
CREATE INDEX if not exists f_ds3 ON mart.f_sales USING btree (customer_id);
CREATE INDEX if not exists f_ds4 ON mart.f_sales USING btree (city_id);
alter table mart.f_sales drop constraint if exists uc_f_sales;
alter table mart.f_sales add constraint uc_f_sales unique(date_id, item_id, customer_id, city_id);

create table if not exists staging.customer_research(
	date_id timestamp,
	category_id int,
	geo_id int,
	sales_qty int,
	sales_amt numeric(14,2)
);

create table if not exists staging.user_activity_log(
	uniq_id varchar(100),
	action_id bigint,
	customer_id bigint,
	date_time timestamp,
	quantity bigint
);

create table if not exists staging.price_log(
	name varchar(100),
	price int
);

create table if not exists mart.f_customer_retention
(
	new_customers_count int,
	returning_customers_count int,
	refunded_customer_count int,
	period_name varchar(100),
	period_id bigint,
	item_id bigint,
	new_customers_revenue bigint,
	returning_customers_revenue bigint,
	customers_refunded int,
	constraint f_customer_retention_pk primary key (period_name, period_id, item_id),
	CONSTRAINT f_customer_retention_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
);