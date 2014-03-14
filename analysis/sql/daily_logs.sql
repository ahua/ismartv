drop table if exists daily_logs;
create table if not exists daily_logs
  (ts float comment 'log timestamp',
   d int comment 'day YYYYMMDD',
   device STRING comment 'device',
   unique_key STRING comment 'unique log key',
   sn STRING comment 'unique device key',
   token string,
   event string,
   duration int,
   clip int,
   code string,
   item int,
   subitem int,
   mediaip string,
   cdn string)
partitioned by (parsets string)
row format delimited fields terminated by ',';

load data local inpath '/home/deploy/src/ismartv/analysis/log/2013122515/20131224.log' 
into table daily_logs partition(parsets='20131224test');


create table if not exists test
  (ts float comment 'log timestamp',
   d int comment 'day YYYYMMDD',
   device STRING comment 'device',
   unique_key STRING comment 'unique log key',
   sn STRING comment 'unique device key',
   token string,
   event string,
   duration int,
   clip int,
   code string,
   item int,
   subitem int,
   mediaip string,
   cdn string)
partitioned by (parsets string)
row format delimited fields terminated by ',';

ALTER TABLE test ADD COLUMNS (isplus int);
alter table test add columns (channel string);
alter table test add columns (quality string);
drop table if exists test;


