create table codesystems (
  oid varchar(50) primary key,
  name varchar(50) not null
);

create table codes (
  id varchar(12) primary key,
  display_name varchar(100) not null,
  code_system varchar(50) not null references codesystems (oid)
);

create table organisations (
  id serial,
  name text not null,
  data jsonb not null
);


