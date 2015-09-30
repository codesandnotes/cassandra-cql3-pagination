
create keyspace if not exists cql3_pagination with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

create table if not exists cql3_pagination.simple_uuid_pk (
  id uuid primary key,
  value text
);

create table if not exists cql3_pagination.simple_string_pk (
  id text primary key,
  value text
);

create table if not exists cql3_pagination.compound_pk (
  country text,
  user text,
  primary key (country, user)
);
