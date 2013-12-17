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
   subitem int)
partitioned by (parsets string)
row format delimited fields terminated by ',';

load data local inpath '/home/deploy/ismartv/output/test/a21.1383042805.log' 
into table daily_logs partition(parsets='201312120420');

