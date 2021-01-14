alter table organisations add column OSNRTH1M integer, add column OSEAST1M integer;
--;;
create index osnrth1m_idx on organisations(OSNRTH1M);
--;;
create index oseast1m_idx on organisations(OSEAST1M);
