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
  id varchar(255) primary key,
  name text not null,
  data jsonb not null
);

create table general_practitioners (
  id varchar(16) primary key,
  name text not null,
  organisation varchar(255) references organisations (id),
  data jsonb not null
);

