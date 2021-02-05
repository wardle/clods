create table updatelog (
id SERIAL,
created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
action varchar not null,
namespace TEXT not null,
identifier TEXT not null,
release_date DATE not null
);