package e2etests

var createUsersTableSQL = `create table "users" (
	id int8 primary key autoincrement,
	email varchar(255) unique,
	name text,
	created timestamp default now()
);`

var createUsersTableIfNotExistsSQL = `create table if not exists "users" (
	id int8 primary key autoincrement,
	email varchar(255) unique,
	name text,
	created timestamp default now()
);`

var createProductsTableSQL = `create table "products" (
	product_id int8 primary key autoincrement,
	name text not null,
	description text,
	price int4 not null,
	created timestamp default now()
);`

var createOrdersTableSQL = `create table "orders" (
	order_id int8 primary key autoincrement,
	user_id int8 not null,
	product_id int4 not null,
	total_paid int4 not null,
	created timestamp default now()
);`
