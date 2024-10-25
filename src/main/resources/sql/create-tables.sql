drop table if exists product;

create table product
(
  id character(9) not null,
  name character varying(50),
  description character varying(255),
  price float,
  update_timestamp timestamp,
  constraint product_pkey primary key (id)
);

create table object1
(
  id character(9) not null,
  field1 character varying(50),
  constraint object1_pkey primary key (id)
);

create table object2
(
  id character(9) not null,
  name character varying(50),
  description character varying(50),
  constraint object2_pkey primary key (id)
);